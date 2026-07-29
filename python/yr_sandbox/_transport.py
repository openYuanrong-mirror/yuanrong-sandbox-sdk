"""HTTP client for the frontend sandbox v1 API.

Small control-plane requests use the unified sandbox action model::

    POST   /api/sandbox/v1/sandboxes
    DELETE /api/sandbox/v1/sandboxes/{sandboxID}
    POST   /api/sandbox/v1/sandboxes/{sandboxID}/invoke   {"action", "args"}

Environment variables::

    YR_SERVER_ADDRESS   host:port of the frontend gateway (required)
    YR_TOKEN            JWT, sent in the ``X-Auth`` header (required)
    YR_GATEWAY_ADDRESS  optional sandbox gateway for tunnel/user port URLs

Response format:
- Auth uses the raw JWT in the ``X-Auth`` header (no ``Bearer`` prefix).
- Frontend responses use ``{"code", "message", "data"}``; ``data`` is a
  base64-encoded JSON result and is decoded by this client.
"""

import base64
import json
import logging
import os
import time
import uuid
from typing import Any, Dict, Iterable, Optional

import httpx

# Default per-call timeout buffer, mirroring types.YR_GET_TIMEOUT_BUFFER.
from ._http_pool import acquire_shared_http_client
from .types import YR_GET_DEFAULT_TIMEOUT, YR_GET_TIMEOUT_BUFFER

logger = logging.getLogger(__name__)


class SandboxError(RuntimeError):
    """Raised when the frontend returns a non-2xx response or an error body."""


_SAFE_DIRECT_FALLBACK_ERRORS = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.PoolTimeout,
)

_CREATE_MAX_ATTEMPTS = 3
_CREATE_RETRY_BACKOFF_SECONDS = 0.1
_CREATE_RETRYABLE_ERRORS = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ReadError,
    httpx.ReadTimeout,
    httpx.RemoteProtocolError,
    httpx.WriteError,
    httpx.WriteTimeout,
)


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value


class SandboxClient:
    """Thin HTTP client over the frontend sandbox v1 control plane.

    A single client is shared by all sub-resources (files/commands/shells)
    of one :class:`~yr_sandbox.Sandbox`. It is also used standalone for
    create/delete before a sandbox id exists.
    """

    def __init__(
        self,
        server: Optional[str] = None,
        token: Optional[str] = None,
        *,
        verify_tls: bool = False,
    ):
        self._server = (server or _require_env("YR_SERVER_ADDRESS")).rstrip("/")
        self._token = token or _require_env("YR_TOKEN")
        # Production gateways are TLS. Set YR_TLS=0 for a plain-HTTP dev
        # cluster (e.g. an AIO frontend started with frontend_ssl_enable=false).
        self._tls = os.environ.get("YR_TLS", "1").strip().lower() not in (
            "0",
            "false",
            "no",
        )
        scheme = "https" if self._tls else "http"
        self._origin = f"{scheme}://{self._server}"
        self._base = f"{scheme}://{self._server}/api/sandbox/v1"
        # The process-level client owns TCP/TLS pooling. Authentication remains
        # on this per-Sandbox lease and is injected into every request.
        self._http = acquire_shared_http_client(
            scheme,
            self._server,
            verify_tls,
            self._token,
        )

        # ── HTTP-direct-via-frontend /direct route ──────────────────────────
        # RRT direct invoke is a control-plane fast path, so it follows the
        # normal frontend gateway (YR_SERVER_ADDRESS / YR_TLS) rather than the
        # data-plane gateway used by tunnel and user port URLs.  The frontend
        # exposes /direct and forwards it to sandboxRouter after frontend JWT
        # auth. The frontend owns the RRT control-port mapping, so clients do
        # not expose the internal RRT port in the URL:
        #   POST {server}/direct/{safeID}/invoke  {action, args}
        self._rrt_port = int(
            os.environ.get("YR_RRT_PORT", "50090").strip() or "50090"
        )
        self._direct_enabled = True
        # Sticky: set once the direct route proves unreachable.
        self._direct_disabled = False
        self._direct_base = f"{scheme}://{self._server}/direct"
        self._last_create: Dict[str, Any] = {}
        self._resume_chunk_size = int(
            os.environ.get("YR_RESUME_CHUNK_SIZE", str(8 * 1024 * 1024))
        )
        self._resume_max_retries = int(os.environ.get("YR_RESUME_MAX_RETRIES", "3"))
        self._resume_min_size = int(
            os.environ.get("YR_RESUME_MIN_SIZE", str(64 * 1024 * 1024))
        )

    # ── lifecycle ──────────────────────────────────────────────────────

    def create(self, body: Dict[str, Any]) -> str:
        """POST /sandboxes — returns the new sandboxID."""
        data = self.create_info(body)
        sid = data.get("sandboxId") or data.get("instanceId")
        if not sid:
            raise SandboxError(f"create response missing sandboxId: {data}")
        return sid

    def resources(self) -> Dict[str, Any]:
        """Query the existing global-scheduler JSON resource endpoint."""
        resp = self._http.get(
            f"{self._origin}/global-scheduler/resources",
            headers={"Type": "json", "Accept": "application/json"},
            timeout=60,
        )
        if resp.status_code >= 400:
            raise SandboxError(
                f"resource query failed: HTTP {resp.status_code} {resp.text}"
            )
        try:
            payload = resp.json()
        except ValueError as exc:
            raise SandboxError("resource query returned invalid JSON") from exc
        if not isinstance(payload, dict):
            raise SandboxError("resource query returned an invalid response")
        return payload

    def create_info(self, body: Dict[str, Any]) -> Dict[str, Any]:
        """POST /sandboxes and return the confirmed-running final SSE result."""
        logical_timeout = int(body.get("createTimeoutSeconds") or 60)
        request_timeout = logical_timeout + YR_GET_TIMEOUT_BUFFER
        # One UUIDv4 supplies 122 random bits for the logical create identity.
        # Reusing it for both fields keeps retries stable without shortening it.
        operation_id = str(uuid.uuid4())
        request_id = f"create-{operation_id}"
        request_body = dict(body)
        request_body.setdefault("name", f"sandbox-{operation_id}")
        deadline = time.monotonic() + request_timeout
        last_error: Optional[BaseException] = None
        attempts = 0

        for attempt in range(1, _CREATE_MAX_ATTEMPTS + 1):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            attempts = attempt
            try:
                data = self._create_info_attempt(
                    request_body,
                    request_id=request_id,
                    request_timeout=remaining,
                )
            except _CREATE_RETRYABLE_ERRORS as exc:
                last_error = exc
                if attempt >= _CREATE_MAX_ATTEMPTS:
                    break
                remaining = deadline - time.monotonic()
                backoff = min(
                    _CREATE_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1)),
                    max(0.0, remaining),
                )
                if backoff <= 0:
                    break
                logger.warning(
                    "sandbox create transport error; retrying "
                    "request_id=%s name=%s attempt=%d/%d error=%s",
                    request_id,
                    request_body["name"],
                    attempt,
                    _CREATE_MAX_ATTEMPTS,
                    exc,
                )
                time.sleep(backoff)
                continue

            self._last_create = data
            return data

        raise SandboxError(
            "sandbox create transport failed after "
            f"{attempts} attempts "
            f"(requestId={request_id}, name={request_body['name']}): "
            f"{last_error or 'create deadline exhausted'}"
        ) from last_error

    def _create_info_attempt(
        self,
        body: Dict[str, Any],
        *,
        request_id: str,
        request_timeout: float,
    ) -> Dict[str, Any]:
        """Execute one transport attempt for a logical sandbox create."""
        final: Optional[Dict[str, Any]] = None
        with self._http.stream(
            "POST",
            f"{self._base}/sandboxes",
            json=body,
            headers={
                "Accept": "text/event-stream",
                "X-Request-Id": request_id,
            },
            timeout=request_timeout,
        ) as resp:
            content_type = resp.headers.get("content-type", "").lower()
            if "text/event-stream" not in content_type:
                resp.read()
                return self._json(resp)
            if resp.status_code >= 400:
                resp.read()
                raise SandboxError(f"HTTP {resp.status_code}: {resp.text}")

            event = ""
            data_lines: list[str] = []
            for line in resp.iter_lines():
                if line == "":
                    if event == "final" and data_lines:
                        try:
                            parsed = json.loads("\n".join(data_lines))
                        except json.JSONDecodeError as exc:
                            raise SandboxError(
                                f"invalid sandbox create final event: {exc}"
                            ) from exc
                        if not isinstance(parsed, dict):
                            raise SandboxError(
                                "sandbox create final event must contain a JSON object"
                            )
                        final = parsed
                        break
                    event = ""
                    data_lines = []
                    continue
                if line.startswith(":"):
                    continue
                if line.startswith("event:"):
                    event = line[6:].strip()
                elif line.startswith("data:"):
                    data_lines.append(line[5:].lstrip())

        if final is None:
            raise SandboxError("sandbox create stream ended before final event")
        if final.get("status") != "running":
            status = final.get("status") or "unknown"
            code = final.get("errorCode")
            message = final.get("message") or "sandbox did not reach running state"
            response_request_id = final.get("requestId") or request_id
            sandbox_id = final.get("sandboxId") or final.get("instanceId") or "unknown"
            raise SandboxError(
                f"sandbox create {status} "
                f"(errorCode={code}, requestId={response_request_id}, "
                f"sandboxId={sandbox_id}): {message}"
            )
        data = final
        return data

    @property
    def last_create(self) -> Dict[str, Any]:
        """Full decoded response from the most recent create call."""
        return self._last_create

    def delete(self, sandbox_id: str) -> None:
        """DELETE /sandboxes/{id}."""
        resp = self._http.delete(f"{self._base}/sandboxes/{sandbox_id}", timeout=60)
        # Treat 404 as a successful idempotent teardown.
        if resp.status_code not in (200, 202, 204, 404):
            raise SandboxError(
                f"delete {sandbox_id} failed: HTTP {resp.status_code} {resp.text}"
            )

    def instance_info(self, sandbox_id: str) -> Dict[str, Any]:
        """Return one instance summary from the existing frontend watcher API."""
        resp = self._http.get(
            f"{self._origin}/api/instances",
            params={"instance_id": sandbox_id},
            timeout=60,
        )
        if resp.status_code >= 400:
            raise SandboxError(
                f"get instance {sandbox_id} failed: "
                f"HTTP {resp.status_code} {resp.text}"
            )
        try:
            payload = resp.json()
        except ValueError as exc:
            raise SandboxError(
                f"get instance {sandbox_id} returned invalid JSON"
            ) from exc
        if not isinstance(payload, list):
            raise SandboxError(
                f"get instance {sandbox_id} returned an invalid response"
            )
        for item in payload:
            if isinstance(item, dict) and item.get("id") == sandbox_id:
                return item
        raise SandboxError(f"sandbox {sandbox_id} was not found")

    # ── unified action invoke ──────────────────────────────────────────

    def invoke(
        self,
        sandbox_id: str,
        action: str,
        args: Optional[Dict[str, Any]] = None,
        *,
        timeout: Optional[int] = None,
    ) -> Dict[str, Any]:
        """POST /sandboxes/{id}/invoke with ``{action, args}``.

        Returns the decoded JSON ``args``/result object. The HTTP timeout is
        derived from the logical *timeout* with a small network buffer, so a slow
        primitive does not trip the client first.
        """
        rpc_timeout: Optional[float]
        if timeout is None:
            rpc_timeout = YR_GET_DEFAULT_TIMEOUT
        elif timeout < 0:
            rpc_timeout = None  # unbounded
        else:
            rpc_timeout = max(timeout + YR_GET_TIMEOUT_BUFFER, YR_GET_DEFAULT_TIMEOUT)

        request_id = self._new_request_id("invoke")

        # Prefer frontend /direct; fall back to frontend invoke.
        if self._direct_enabled and not self._direct_disabled:
            result, fell_back = self._invoke_direct(
                sandbox_id,
                action,
                args or {},
                rpc_timeout,
                request_id,
            )
            if not fell_back:
                return result

        resp = self._http.post(
            f"{self._base}/sandboxes/{sandbox_id}/invoke",
            json={
                "action": action,
                "args": args or {},
                "requestId": request_id,
            },
            timeout=rpc_timeout,
            headers={"X-YR-Request-ID": request_id},
        )
        return self._json(resp)

    def _invoke_direct(
        self,
        sandbox_id: str,
        action: str,
        args: Dict[str, Any],
        rpc_timeout: Optional[float],
        request_id: str,
    ) -> "tuple[Dict[str, Any], bool]":
        """Try the frontend /direct path. Returns ``(result, fell_back)``.

        ``fell_back=True`` is returned only when the request is known not to
        have reached RRT (connect/pool failure) or when frontend reports that
        the direct route does not exist. Failures after request transmission
        have an unknown execution outcome and must not retry side effects.
        Unlike frontend invoke, the RRT HTTP server returns the raw result JSON
        (no base64 ``BuildJobResponse`` envelope); action-level errors live
        inside that object (HTTP 200).
        """
        url = f"{self._direct_base}/{self._safe_id(sandbox_id)}/invoke"
        try:
            resp = self._http.post(
                url,
                json={"action": action, "args": args, "requestId": request_id},
                timeout=rpc_timeout,
                headers={"X-YR-Request-ID": request_id},
            )
        except _SAFE_DIRECT_FALLBACK_ERRORS:
            # No connection was established and no request bytes reached RRT.
            self._direct_disabled = True
            return {}, True
        except httpx.RequestError as exc:
            self._direct_disabled = True
            raise SandboxError(
                "direct invoke outcome is unknown after transport failure "
                f"(requestId={request_id}): {exc}"
            ) from exc
        if resp.status_code == 404:
            self._direct_disabled = True
            return {}, True
        if resp.status_code >= 500:
            self._direct_disabled = True
            raise SandboxError(
                "direct invoke outcome is unknown after "
                f"HTTP {resp.status_code} (requestId={request_id}): {resp.text}"
            )
        if resp.status_code >= 400:
            raise SandboxError(
                f"direct invoke failed: HTTP {resp.status_code} "
                f"(requestId={request_id}): {resp.text}"
            )
        try:
            parsed = resp.json()
        except ValueError as exc:
            self._direct_disabled = True
            raise SandboxError(
                "direct invoke returned invalid JSON; outcome is unknown "
                f"(requestId={request_id})"
            ) from exc
        if not isinstance(parsed, dict):
            parsed = {"value": parsed}
        return parsed, False

    def upload_file_direct(
        self,
        sandbox_id: str,
        local_path: str,
        remote_path: str,
        rpc_timeout: Optional[float] = None,
        *,
        upload_type: str = "file",
    ) -> Dict[str, Any]:
        """Upload a file/tar over the required frontend /direct binary data path."""
        content_len = os.path.getsize(local_path)
        if upload_type == "file" and content_len >= self._resume_min_size:
            return self._upload_file_resumable(
                sandbox_id, local_path, remote_path, rpc_timeout
            )
        with open(local_path, "rb") as f:
            return self._upload_direct(
                sandbox_id,
                f,
                remote_path,
                rpc_timeout,
                upload_type=upload_type,
                content_len=content_len,
            )

    def _upload_file_resumable(
        self,
        sandbox_id: str,
        local_path: str,
        remote_path: str,
        rpc_timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Upload a file via resumable /direct chunks and atomic commit."""
        total = os.path.getsize(local_path)
        upload_id = self._new_request_id("upload")
        offset = self._upload_status(sandbox_id, remote_path, upload_id, rpc_timeout)
        with open(local_path, "rb") as f:
            while offset < total:
                f.seek(offset)
                chunk = f.read(min(self._resume_chunk_size, total - offset))
                if not chunk:
                    break
                attempts = 0
                while True:
                    try:
                        result = self._upload_direct(
                            sandbox_id,
                            chunk,
                            remote_path,
                            rpc_timeout,
                            upload_type="file",
                            content_len=len(chunk),
                            extra_params={
                                "uploadId": upload_id,
                                "offset": str(offset),
                                "totalSize": str(total),
                            },
                        )
                        offset = int(result.get("offset", offset + len(chunk)))
                        break
                    except SandboxError:
                        attempts += 1
                        if attempts > self._resume_max_retries:
                            raise
                        offset = self._upload_status(
                            sandbox_id, remote_path, upload_id, rpc_timeout
                        )
            return self._upload_commit(sandbox_id, remote_path, upload_id, total, rpc_timeout)

    def _upload_status(
        self,
        sandbox_id: str,
        remote_path: str,
        upload_id: str,
        rpc_timeout: Optional[float],
    ) -> int:
        url = f"{self._direct_base}/{self._safe_id(sandbox_id)}/upload/status"
        try:
            resp = self._http.get(
                url,
                params={"path": remote_path, "uploadId": upload_id},
                timeout=rpc_timeout,
            )
        except httpx.RequestError as e:
            raise SandboxError(f"direct upload status {sandbox_id} failed: {e}") from e
        if resp.status_code >= 400:
            raise SandboxError(
                f"direct upload status {sandbox_id} failed: HTTP {resp.status_code} {resp.text}"
            )
        try:
            parsed = resp.json()
        except ValueError as e:
            raise SandboxError("direct upload status returned non-JSON body") from e
        return int(parsed.get("offset", 0))

    def _upload_commit(
        self,
        sandbox_id: str,
        remote_path: str,
        upload_id: str,
        total: int,
        rpc_timeout: Optional[float],
    ) -> Dict[str, Any]:
        url = f"{self._direct_base}/{self._safe_id(sandbox_id)}/upload/commit"
        request_id = self._new_request_id("upload-commit")
        try:
            resp = self._http.post(
                url,
                params={
                    "path": remote_path,
                    "uploadId": upload_id,
                    "totalSize": str(total),
                },
                timeout=rpc_timeout,
                headers={"X-YR-Request-ID": request_id},
            )
        except httpx.RequestError as e:
            raise SandboxError(f"direct upload commit {sandbox_id} failed: {e}") from e
        if resp.status_code >= 400:
            raise SandboxError(
                f"direct upload commit {sandbox_id} failed: HTTP {resp.status_code} {resp.text}"
            )
        try:
            parsed = resp.json()
        except ValueError as e:
            raise SandboxError("direct upload commit returned non-JSON body") from e
        if not isinstance(parsed, dict):
            parsed = {"value": parsed}
        return parsed

    def upload_bytes_direct(
        self,
        sandbox_id: str,
        data: bytes,
        remote_path: str,
        rpc_timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Upload bytes over the required frontend /direct binary data path."""
        return self._upload_direct(
            sandbox_id, data, remote_path, rpc_timeout, content_len=len(data)
        )

    def upload_stream_direct(
        self,
        sandbox_id: str,
        chunks: Iterable[bytes],
        remote_path: str,
        rpc_timeout: Optional[float] = None,
        *,
        upload_type: str = "file",
    ) -> Dict[str, Any]:
        """Upload an iterator over bytes using HTTP chunked transfer encoding."""
        return self._upload_direct(
            sandbox_id,
            chunks,
            remote_path,
            rpc_timeout,
            upload_type=upload_type,
        )

    def _upload_direct(
        self,
        sandbox_id: str,
        content,
        remote_path: str,
        rpc_timeout: Optional[float],
        *,
        upload_type: str = "file",
        content_len: Optional[int] = None,
        extra_params: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        url = f"{self._direct_base}/{self._safe_id(sandbox_id)}/upload"
        content_type = "application/x-tar" if upload_type == "tar" else "application/octet-stream"
        headers = {"Content-Type": content_type}
        if content_len is not None:
            headers["Content-Length"] = str(content_len)
        try:
            params = {"path": remote_path, "type": upload_type}
            if extra_params:
                params.update(extra_params)
            resp = self._http.post(
                url,
                params=params,
                content=content,
                timeout=rpc_timeout,
                headers=headers,
            )
        except httpx.RequestError as e:
            raise SandboxError(f"direct upload {sandbox_id} failed: {e}") from e
        if resp.status_code >= 400:
            raise SandboxError(
                f"direct upload {sandbox_id} failed: HTTP {resp.status_code} {resp.text}"
            )
        try:
            parsed = resp.json()
        except ValueError as e:
            raise SandboxError(f"direct upload {sandbox_id} returned non-JSON body") from e
        if not isinstance(parsed, dict):
            parsed = {"value": parsed}
        return parsed

    def download_file_direct(
        self,
        sandbox_id: str,
        remote_path: str,
        local_path: str,
        rpc_timeout: Optional[float] = None,
        *,
        download_type: str = "file",
    ) -> None:
        """Download a file/tar over the required frontend /direct binary data path."""
        if download_type == "file":
            return self._download_file_resumable(
                sandbox_id, remote_path, local_path, rpc_timeout
            )
        url = f"{self._direct_base}/{self._safe_id(sandbox_id)}/download"
        try:
            with self._http.stream(
                "GET",
                url,
                params={"path": remote_path, "type": download_type},
                timeout=rpc_timeout,
            ) as resp:
                if resp.status_code >= 400:
                    body = resp.read().decode("utf-8", errors="replace")
                    raise SandboxError(
                        f"direct download {sandbox_id} failed: HTTP {resp.status_code} {body}"
                    )
                self._write_stream_to_file(resp, local_path, append=False)
        except httpx.RequestError as e:
            raise SandboxError(f"direct download {sandbox_id} failed: {e}") from e

    def _download_file_resumable(
        self,
        sandbox_id: str,
        remote_path: str,
        local_path: str,
        rpc_timeout: Optional[float] = None,
    ) -> None:
        url = f"{self._direct_base}/{self._safe_id(sandbox_id)}/download"
        part_path = f"{local_path}.part"
        parent = os.path.dirname(os.path.abspath(local_path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        attempts = 0
        while True:
            offset = os.path.getsize(part_path) if os.path.exists(part_path) else 0
            headers = {"Range": f"bytes={offset}-"} if offset > 0 else None
            try:
                with self._http.stream(
                    "GET",
                    url,
                    params={"path": remote_path, "type": "file"},
                    timeout=rpc_timeout,
                    headers=headers,
                ) as resp:
                    if resp.status_code >= 400:
                        body = resp.read().decode("utf-8", errors="replace")
                        raise SandboxError(
                            f"direct download {sandbox_id} failed: HTTP {resp.status_code} {body}"
                        )
                    append = offset > 0 and resp.status_code == 206
                    if offset > 0 and resp.status_code != 206:
                        append = False
                    self._write_stream_to_file(resp, part_path, append=append)
                os.replace(part_path, local_path)
                return
            except httpx.RequestError as e:
                attempts += 1
                if attempts > self._resume_max_retries:
                    raise SandboxError(f"direct download {sandbox_id} failed: {e}") from e

    @staticmethod
    def _write_stream_to_file(resp: httpx.Response, path: str, *, append: bool) -> None:
        mode = "ab" if append else "wb"
        with open(path, mode) as f:
            for chunk in resp.iter_bytes():
                if chunk:
                    f.write(chunk)

    def download_bytes_direct(
        self,
        sandbox_id: str,
        remote_path: str,
        rpc_timeout: Optional[float] = None,
    ) -> bytes:
        """Download bytes over the required frontend /direct binary data path."""
        url = f"{self._direct_base}/{self._safe_id(sandbox_id)}/download"
        try:
            resp = self._http.get(
                url,
                params={"path": remote_path, "type": "file"},
                timeout=rpc_timeout,
            )
        except httpx.RequestError as e:
            raise SandboxError(f"direct download {sandbox_id} failed: {e}") from e
        if resp.status_code >= 400:
            raise SandboxError(
                f"direct download {sandbox_id} failed: HTTP {resp.status_code} {resp.text}"
            )
        return resp.content

    @property
    def direct_enabled(self) -> bool:
        """Whether RRT direct invoke first tries the frontend /direct route."""
        return self._direct_enabled

    @property
    def rrt_port(self) -> int:
        """Internal RRT HTTP container port requested during sandbox create."""
        return self._rrt_port

    @staticmethod
    def _new_request_id(prefix: str) -> str:
        return f"{prefix}-{uuid.uuid4()}"

    @staticmethod
    def _safe_id(sandbox_id: str) -> str:
        """Sanitize an instance id the same way the router does (route.SanitizeID):
        ``@`` -> ``-at-`` and ``/`` ``.`` ``_`` -> ``-``."""
        s = sandbox_id.replace("@", "-at-")
        return "".join("-" if c in "/._" else c for c in s)

    @property
    def token(self) -> str:
        return self._token

    def close(self) -> None:
        self._http.close()

    # ── internal ───────────────────────────────────────────────────────

    @staticmethod
    def _json(resp: httpx.Response) -> Dict[str, Any]:
        """Unwrap the job.BuildJobResponse envelope.

        Body: ``{"code": <http-status>, "message": "<err>", "data": "<b64>"}``.
        ``code`` mirrors the HTTP status; HTTP-level failures (>=400) raise.
        Action-level errors are carried *inside* ``data`` (e.g. an ``error``
        key), so they are NOT raised here — the caller's result parsing
        handles them while preserving the requested local file layout.
        """
        if resp.status_code >= 400:
            raise SandboxError(f"HTTP {resp.status_code}: {resp.text}")
        try:
            envelope = resp.json()
        except ValueError:
            return {}

        code = envelope.get("code", resp.status_code)
        if isinstance(code, int) and code >= 400:
            raise SandboxError(f"code {code}: {envelope.get('message', '')}")

        raw = envelope.get("data")
        if raw in (None, ""):
            return {}
        # Go marshals []byte as base64; decode then JSON-parse the inner result.
        if isinstance(raw, str):
            try:
                decoded = base64.b64decode(raw)
                parsed = json.loads(decoded)
            except (ValueError, json.JSONDecodeError) as e:
                raise SandboxError(f"failed to decode response data: {e}") from e
        else:
            # Some deployments may already return a JSON object for data.
            parsed = raw
        return parsed if isinstance(parsed, dict) else {"value": parsed}
