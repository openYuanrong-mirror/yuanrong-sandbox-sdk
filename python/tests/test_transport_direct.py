"""Offline tests for the HTTP-direct-via-frontend /direct path in SandboxClient.

No cluster required: an httpx.MockTransport stands in for both the direct route
(raw-result JSON) and the frontend (base64 BuildJobResponse envelope), so we
can assert direct-vs-fallback routing, response parsing, and the sticky
disable. Run: ``python tests/test_transport_direct.py`` (or via pytest).
"""

import base64
import hashlib
import importlib.util
import inspect
import io
import json
import os
import tarfile
import tempfile
import urllib.request
from pathlib import Path

import httpx

os.environ.setdefault("YR_SERVER_ADDRESS", "frontend:8889")
os.environ.setdefault("YR_TOKEN", "test-token")

from yr_sandbox._transport import SandboxClient, SandboxError  # noqa: E402
from yr_sandbox.filesystem import Filesystem  # noqa: E402


def _check(cond: bool, msg: str) -> None:
    """Repo tests avoid bare ``assert`` (stripped under -O); raise explicitly."""
    if not cond:
        raise AssertionError(msg)


def _envelope(inner: dict) -> dict:
    """frontend BuildJobResponse: data is base64(JSON(inner))."""
    raw = base64.b64encode(json.dumps(inner).encode()).decode()
    return {"code": 200, "message": "", "data": raw}


def _make_client(handler):
    """A SandboxClient with frontend /direct enabled and a mocked transport."""
    os.environ.pop("YR_GATEWAY_ADDRESS", None)
    os.environ["YR_TLS"] = "0"
    c = SandboxClient()
    c._http = httpx.Client(
        transport=httpx.MockTransport(handler), headers={"X-Auth": "test-token"}
    )
    return c


def test_create_uses_sse_and_returns_running_final():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["accept"] = request.headers.get("accept")
        seen["body"] = json.loads(request.read())
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            text=(
                'event: accepted\n'
                'data: {"status":"creating"}\n\n'
                ': heartbeat\n\n'
                'event: final\n'
                'data: {"sandboxId":"sandbox-sse","status":"running"}\n\n'
            ),
        )

    c = _make_client(handler)
    result = c.create_info({"name": "sandbox-sse", "createTimeoutSeconds": 7})
    _check(result["sandboxId"] == "sandbox-sse", f"create result: {result}")
    _check(result["status"] == "running", f"create status: {result}")
    _check(seen["accept"] == "text/event-stream", f"Accept header: {seen}")
    _check(seen["body"]["createTimeoutSeconds"] == 7, f"request body: {seen}")
    print("ok: create uses SSE and returns running final")


def test_create_rejects_timeout_final():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["request_id"] = request.headers.get("X-Request-Id")
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            text=(
                'event: accepted\n'
                'data: {"status":"creating"}\n\n'
                'event: final\n'
                'data: {"sandboxId":"sandbox-timeout","status":"timeout",'
                '"errorCode":3002,"message":"create timed out"}\n\n'
            ),
        )

    c = _make_client(handler)
    try:
        c.create_info({"createTimeoutSeconds": 3})
    except SandboxError as exc:
        _check("create timed out" in str(exc), f"timeout error: {exc}")
        _check("3002" in str(exc), f"timeout code missing: {exc}")
        _check(
            f"requestId={seen['request_id']}" in str(exc),
            f"request id missing: {exc}",
        )
        _check(
            "sandboxId=sandbox-timeout" in str(exc),
            f"sandbox id missing: {exc}",
        )
    else:
        raise AssertionError("timeout final must raise SandboxError")
    print("ok: create rejects timeout final")


def test_create_rejects_stream_without_final():
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            text='event: accepted\ndata: {"status":"creating"}\n\n: heartbeat\n\n',
        )

    c = _make_client(handler)
    try:
        c.create_info({"createTimeoutSeconds": 3})
    except SandboxError as exc:
        _check("final" in str(exc).lower(), f"missing-final error: {exc}")
    else:
        raise AssertionError("stream without final must raise SandboxError")
    print("ok: create rejects stream without final")


def test_sandbox_create_timeout_precedence_and_body():
    import yr_sandbox.sandbox_api as sandbox_api

    original_client = sandbox_api.SandboxClient
    original_env = os.environ.get("YR_SANDBOX_CREATE_TIMEOUT")
    seen = []

    class FakeClient:
        def create_info(self, body):
            seen.append(dict(body))
            return {"sandboxId": f"sandbox-{len(seen)}", "status": "running"}

        def delete(self, _sandbox_id):
            pass

        def close(self):
            pass

    try:
        sandbox_api.SandboxClient = FakeClient
        os.environ["YR_SANDBOX_CREATE_TIMEOUT"] = "90"
        explicit = sandbox_api.Sandbox(
            image="python:3.12-slim",
            create_timeout=70,
            runtime="kata",
            detached=True,
        )
        schedule_only = sandbox_api.Sandbox(
            image="python:3.12-slim",
            schedule_timeout=45,
            detached=True,
        )
        inherited = sandbox_api.Sandbox(image="python:3.12-slim", detached=True)
        explicit.kill()
        schedule_only.kill()
        inherited.kill()
    finally:
        sandbox_api.SandboxClient = original_client
        if original_env is None:
            os.environ.pop("YR_SANDBOX_CREATE_TIMEOUT", None)
        else:
            os.environ["YR_SANDBOX_CREATE_TIMEOUT"] = original_env

    _check(seen[0]["createTimeoutSeconds"] == 70, f"explicit timeout body: {seen[0]}")
    _check(seen[0]["scheduleTimeoutSeconds"] == 30, f"default schedule timeout body: {seen[0]}")
    _check(seen[0]["runtime"] == "kata", f"runtime body: {seen[0]}")
    _check(seen[1]["createTimeoutSeconds"] == 90, f"env create timeout body: {seen[1]}")
    _check(seen[1]["scheduleTimeoutSeconds"] == 45, f"explicit schedule timeout body: {seen[1]}")
    _check(seen[2]["createTimeoutSeconds"] == 90, f"env timeout body: {seen[2]}")
    _check(seen[2]["scheduleTimeoutSeconds"] == 30, f"default schedule timeout body: {seen[2]}")
    _check(seen[2]["runtime"] == "runsc", f"default isolation runtime body: {seen[2]}")
    print("ok: Sandbox create timeout precedence and body")


def test_sandbox_create_timeout_validation():
    import yr_sandbox.sandbox_api as sandbox_api

    invalid = (
        (
            {"create_timeout": 30},
            "create_timeout - schedule_timeout must be at least 30",
        ),
        ({"schedule_timeout": -1}, "schedule_timeout must be a positive integer"),
        ({"schedule_timeout": 0}, "schedule_timeout must be a positive integer"),
        (
            {"create_timeout": 60, "schedule_timeout": 70},
            "schedule_timeout must be less than or equal to create_timeout",
        ),
        (
            {"create_timeout": 60, "schedule_timeout": 45},
            "create_timeout - schedule_timeout must be at least 30",
        ),
    )
    for kwargs, message in invalid:
        try:
            sandbox_api.Sandbox(
                image="python:3.12-slim",
                detached=True,
                **kwargs,
            )
        except ValueError as exc:
            _check(str(exc) == message, f"unexpected validation error: {exc}")
        else:
            raise AssertionError(f"invalid timeout combination accepted: {kwargs}")
    print("ok: Sandbox create timeout validation")


def test_direct_success_no_fallback():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path.startswith("/api/sandbox/v1"):
            raise AssertionError("frontend must NOT be called on direct success")
        # frontend /direct raw-result path: /direct/{safeID}/invoke
        return httpx.Response(200, json={"exists": True})

    c = _make_client(handler)
    out = c.invoke("sandbox-demo", "file.exists", {"path": "/"})
    _check(out == {"exists": True}, f"direct result: {out}")
    _check(calls == ["/direct/sandbox-demo/invoke"], f"calls: {calls}")
    print("ok: direct success, no fallback ->", calls)


def test_direct_5xx_does_not_retry_current_invoke_and_sticks():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        calls.append(
            (
                request.url.path,
                request.headers.get("x-yr-request-id"),
                body.get("requestId"),
            )
        )
        if request.url.path.startswith("/api/sandbox/v1"):
            return httpx.Response(200, json=_envelope({"exists": True}))
        return httpx.Response(503, json={"error": "route unavailable"})

    c = _make_client(handler)
    try:
        c.invoke("sandbox-demo", "process.exec", {"cmd": ["touch", "/tmp/once"]})
    except SandboxError as exc:
        _check("outcome is unknown" in str(exc), f"unexpected 5xx error: {exc}")
        _check(calls[0][1] in str(exc), f"request id missing from error: {exc}")
    else:
        raise AssertionError("5xx direct invoke must not retry through frontend")
    _check(
        calls == [
            (
                "/direct/sandbox-demo/invoke",
                calls[0][1],
                calls[0][1],
            )
        ],
        f"current invoke must be sent once: {calls}",
    )
    _check(c._direct_disabled is True, "5xx should sticky-disable direct")

    # A later logical invocation may use the fallback path.
    out = c.invoke("sandbox-demo", "file.exists", {"path": "/"})
    _check(out == {"exists": True}, f"later fallback result: {out}")
    _check(
        calls[1][0].startswith("/api/sandbox/v1"),
        f"later call should skip direct: {calls}",
    )
    _check(
        calls[1][1] == calls[1][2] and calls[1][1],
        f"fallback request id should be observable: {calls}",
    )
    print("ok: 5xx is not retried; later invoke uses sticky fallback ->", calls)


def test_direct_read_timeout_does_not_retry_unknown_outcome():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        calls.append(
            (
                request.url.path,
                request.headers.get("x-yr-request-id"),
                body.get("requestId"),
            )
        )
        if request.url.path.startswith("/api/sandbox/v1"):
            return httpx.Response(200, json=_envelope({"ok": True}))
        raise httpx.ReadTimeout(
            "response lost after possible execution",
            request=request,
        )

    c = _make_client(handler)
    try:
        c.invoke("sandbox-demo", "process.start", {"cmd": ["side-effect"]})
    except SandboxError as exc:
        _check("outcome is unknown" in str(exc), f"unexpected timeout error: {exc}")
        _check(calls[0][1] in str(exc), f"request id missing from error: {exc}")
    else:
        raise AssertionError("read timeout must not retry an unknown outcome")
    _check(
        len(calls) == 1 and calls[0][0] == "/direct/sandbox-demo/invoke",
        f"read timeout retried the action: {calls}",
    )
    _check(c._direct_disabled is True, "read timeout should sticky-disable direct")
    print("ok: read timeout does not retry unknown outcome ->", calls)


def test_direct_4xx_does_not_fallback():
    for status in (400, 401, 403, 409, 429):
        calls = []

        def handler(
            request: httpx.Request,
            status=status,
            calls=calls,
        ) -> httpx.Response:
            calls.append(request.url.path)
            if request.url.path.startswith("/api/sandbox/v1"):
                return httpx.Response(200, json=_envelope({"ok": True}))
            return httpx.Response(status, json={"error": f"status-{status}"})

        c = _make_client(handler)
        try:
            c.invoke("sandbox-demo", "process.exec", {"cmd": ["side-effect"]})
        except SandboxError as exc:
            _check(f"HTTP {status}" in str(exc), f"unexpected {status} error: {exc}")
        else:
            raise AssertionError(f"HTTP {status} must not fall back")
        _check(
            calls == ["/direct/sandbox-demo/invoke"],
            f"HTTP {status} retried through frontend: {calls}",
        )
        _check(c._direct_disabled is False, f"HTTP {status} should not disable direct")
    print("ok: direct 4xx responses do not fall back")


def test_direct_invalid_json_does_not_retry_unknown_outcome():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path.startswith("/api/sandbox/v1"):
            return httpx.Response(200, json=_envelope({"ok": True}))
        return httpx.Response(200, content=b"not-json")

    c = _make_client(handler)
    try:
        c.invoke("sandbox-demo", "process.exec", {"cmd": ["side-effect"]})
    except SandboxError as exc:
        _check("invalid JSON" in str(exc), f"unexpected invalid JSON error: {exc}")
    else:
        raise AssertionError("invalid direct response must not retry")
    _check(
        calls == ["/direct/sandbox-demo/invoke"],
        f"invalid direct response retried through frontend: {calls}",
    )
    _check(c._direct_disabled is True, "invalid response should disable direct")
    print("ok: invalid direct response does not retry unknown outcome")


def test_direct_connect_error_falls_back():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        calls.append(
            (
                request.url.path,
                request.headers.get("x-yr-request-id"),
                body.get("requestId"),
            )
        )
        if request.url.path.startswith("/api/sandbox/v1"):
            return httpx.Response(200, json=_envelope({"ok": 1}))
        raise httpx.ConnectError("direct route down")

    c = _make_client(handler)
    out = c.invoke("sandbox-demo", "file.exists", {"path": "/"})
    _check(out == {"ok": 1}, f"connect-error fallback: {out}")
    _check(c._direct_disabled is True, "connect error should sticky-disable")
    _check(
        calls[0][0] == "/direct/sandbox-demo/invoke",
        f"first call direct: {calls}",
    )
    _check(calls[1][0].startswith("/api/sandbox/v1"), f"fallback: {calls}")
    _check(
        calls[0][1] == calls[0][2] == calls[1][1] == calls[1][2],
        f"fallback must reuse the logical request id: {calls}",
    )
    print("ok: connect-error fallback ->", calls)


def test_direct_fallback_when_frontend_direct_missing():
    os.environ.pop("YR_GATEWAY_ADDRESS", None)
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        calls.append(
            (
                request.url.path,
                request.headers.get("x-yr-request-id"),
                body.get("requestId"),
            )
        )
        if request.url.path.startswith("/api/sandbox/v1"):
            return httpx.Response(200, json=_envelope({"exists": False}))
        return httpx.Response(404, json={"error": "direct route missing"})

    os.environ["YR_TLS"] = "0"
    c = SandboxClient()
    c._http = httpx.Client(
        transport=httpx.MockTransport(handler), headers={"X-Auth": "t"}
    )
    _check(
        c.direct_enabled is True,
        "direct should default on through frontend /direct",
    )
    out = c.invoke("sandbox-demo", "file.exists", {"path": "/"})
    _check(out == {"exists": False}, f"frontend-only result: {out}")
    _check(
        calls[0][0] == "/direct/sandbox-demo/invoke",
        f"first call direct: {calls}",
    )
    _check(calls[1][0].startswith("/api/sandbox/v1"), f"fallback: {calls}")
    _check(
        calls[0][1] == calls[0][2] == calls[1][1] == calls[1][2],
        f"404 fallback must reuse the logical request id: {calls}",
    )
    print("ok: missing frontend /direct -> fallback ->", calls)


def test_direct_binary_upload_success():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.read()
        calls.append((request.url.path, request.url.params.get("path"), body))
        if request.url.path.startswith("/api/sandbox/v1"):
            raise AssertionError("binary upload must not use frontend invoke")
        return httpx.Response(
            200,
            json={
                "error": None,
                "name": "blob.bin",
                "path": request.url.params.get("path"),
                "type": "file",
                "size": len(body),
            },
        )

    c = _make_client(handler)
    out = c.upload_bytes_direct("sandbox-demo", b"binary-payload", "/tmp/blob.bin")
    _check(out["size"] == len(b"binary-payload"), f"upload result: {out}")
    _check(
        calls == [("/direct/sandbox-demo/upload", "/tmp/blob.bin", b"binary-payload")],
        f"upload calls: {calls}",
    )
    print("ok: direct binary upload ->", calls)


def test_files_write_bytes_uses_direct_upload():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.read()
        calls.append((request.url.path, request.url.params.get("path"), len(body)))
        _check(
            request.url.path == "/direct/sandbox-demo/upload",
            f"bytes write should use upload, got {request.url.path}",
        )
        return httpx.Response(
            200,
            json={
                "error": None,
                "name": "large.bin",
                "path": request.url.params.get("path"),
                "type": "file",
                "size": len(body),
            },
        )

    c = _make_client(handler)
    info = Filesystem(c, "sandbox-demo").write("/tmp/large.bin", b"x")
    _check(info.size == 1, f"upload-backed write size: {info}")
    _check(calls == [("/direct/sandbox-demo/upload", "/tmp/large.bin", 1)], f"calls: {calls}")
    print("ok: bytes write uses direct upload ->", calls)


def test_files_write_text_uses_direct_upload():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.read()
        calls.append((request.url.path, request.url.params.get("path"), body))
        _check(
            request.url.path == "/direct/sandbox-demo/upload",
            f"text write should use upload, got {request.url.path}",
        )
        return httpx.Response(
            200,
            json={
                "error": None,
                "name": "text.txt",
                "path": request.url.params.get("path"),
                "type": "file",
                "size": len(body),
            },
        )

    c = _make_client(handler)
    info = Filesystem(c, "sandbox-demo").write("/tmp/text.txt", "héllo")
    _check(info.size == len("héllo".encode("utf-8")), f"upload-backed text size: {info}")
    _check(
        calls == [("/direct/sandbox-demo/upload", "/tmp/text.txt", "héllo".encode("utf-8"))],
        f"calls: {calls}",
    )
    print("ok: text write uses direct upload ->", calls)


def test_copy_from_local_small_file_uses_single_direct_upload():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.read()
        calls.append(
            (
                request.method,
                request.url.path,
                request.url.params.get("path"),
                request.url.params.get("uploadId"),
                body,
            )
        )
        return httpx.Response(
            200,
            json={
                "error": None,
                "name": "up.dat",
                "path": request.url.params.get("path"),
                "type": "file",
                "size": len(body),
            },
        )

    c = _make_client(handler)
    with tempfile.NamedTemporaryFile("wb", delete=True) as f:
        f.write(b"upload-file")
        f.flush()
        Filesystem(c, "sandbox-demo").copy_from_local(f.name, "/tmp/up.dat")

    _check(
        calls == [
            (
                "POST",
                "/direct/sandbox-demo/upload",
                "/tmp/up.dat",
                None,
                b"upload-file",
            )
        ],
        f"small file should use one upload request: {calls}",
    )
    print("ok: copy_from_local small file uses single upload ->", calls)


def test_copy_from_local_file_uses_resumable_direct_upload_above_threshold():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.read()
        calls.append(
            (
                request.method,
                request.url.path,
                request.url.params.get("path"),
                request.url.params.get("uploadId") is not None,
                request.url.params.get("offset"),
                body,
            )
        )
        if request.method == "GET" and request.url.path.endswith("/upload/status"):
            return httpx.Response(200, json={"error": None, "offset": 0})
        if request.method == "POST" and request.url.path.endswith("/upload/commit"):
            return httpx.Response(
                200,
                json={
                    "error": None,
                    "name": "up.dat",
                    "path": request.url.params.get("path"),
                    "type": "file",
                    "size": len(b"upload-file"),
                    "committed": True,
                },
            )
        return httpx.Response(
            200,
            json={
                "error": None,
                "path": request.url.params.get("path"),
                "type": "file",
                "offset": len(body),
                "bytes_written": len(body),
            },
        )

    c = _make_client(handler)
    c._resume_min_size = 1
    with tempfile.NamedTemporaryFile("wb", delete=True) as f:
        f.write(b"upload-file")
        f.flush()
        Filesystem(c, "sandbox-demo").copy_from_local(f.name, "/tmp/up.dat")

    _check(
        calls[0][0:3] == ("GET", "/direct/sandbox-demo/upload/status", "/tmp/up.dat"),
        f"status call: {calls}",
    )
    _check(
        calls[1][0:5]
        == ("POST", "/direct/sandbox-demo/upload", "/tmp/up.dat", True, "0"),
        f"chunk call: {calls}",
    )
    _check(calls[1][5] == b"upload-file", f"uploaded body: {calls}")
    _check(
        calls[2][0:3]
        == ("POST", "/direct/sandbox-demo/upload/commit", "/tmp/up.dat"),
        f"commit call: {calls}",
    )
    print("ok: copy_from_local large file uses resumable upload ->", calls)

def test_files_read_uses_direct_download():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path, request.url.params.get("path")))
        _check(
            request.url.path == "/direct/sandbox-demo/download",
            f"read should use download, got {request.url.path}",
        )
        return httpx.Response(200, content=b"hello\xff")

    c = _make_client(handler)
    fs = Filesystem(c, "sandbox-demo")
    _check(
        fs.read("/tmp/read.bin", format="bytes") == b"hello\xff",
        "bytes read mismatch",
    )
    _check(fs.read("/tmp/read.bin") == "hello�", "text read mismatch")
    _check(
        calls == [
            ("GET", "/direct/sandbox-demo/download", "/tmp/read.bin"),
            ("GET", "/direct/sandbox-demo/download", "/tmp/read.bin"),
        ],
        f"download calls: {calls}",
    )
    print("ok: files.read uses direct download ->", calls)


def test_copy_to_local_file_uses_direct_download():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path, request.url.params.get("path")))
        if request.url.path.endswith("/invoke"):
            return httpx.Response(
                200,
                json={
                    "name": "down.dat",
                    "path": "/tmp/down.dat",
                    "type": "file",
                    "size": 13,
                    "permissions": "",
                    "modified_time": 0,
                },
            )
        return httpx.Response(200, content=b"download-file")

    c = _make_client(handler)
    with tempfile.NamedTemporaryFile(delete=True) as f:
        target = f.name
    Filesystem(c, "sandbox-demo").copy_to_local("/tmp/down.dat", target)
    try:
        with open(target, "rb") as f:
            _check(f.read() == b"download-file", "downloaded file mismatch")
    finally:
        if os.path.exists(target):
            os.unlink(target)

    _check(calls[0][1] == "/direct/sandbox-demo/invoke", f"stat call: {calls}")
    _check(calls[1] == ("GET", "/direct/sandbox-demo/download", "/tmp/down.dat"), f"download call: {calls}")
    print("ok: copy_to_local file uses direct download ->", calls)


def test_copy_from_local_dir_streams_direct_tar_upload():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = request.read()
        calls.append(
            (
                request.url.path,
                request.url.params.get("path"),
                request.url.params.get("type"),
                request.headers.get("content-length"),
                request.headers.get("transfer-encoding"),
                body,
            )
        )
        names = []
        with tarfile.open(fileobj=io.BytesIO(body), mode="r") as tar:
            names = sorted(m.name for m in tar.getmembers())
        _check(names == ["a.txt"], f"tar names: {names}")
        return httpx.Response(
            200,
            json={"error": None, "name": "remote-dir", "path": request.url.params.get("path"), "type": "dir", "size": len(body)},
        )

    c = _make_client(handler)
    with tempfile.TemporaryDirectory() as d:
        with open(os.path.join(d, "a.txt"), "wb") as f:
            f.write(b"dir-file")
        Filesystem(c, "sandbox-demo").copy_from_local(d, "/tmp/remote-dir")

    _check(calls[0][0:3] == ("/direct/sandbox-demo/upload", "/tmp/remote-dir", "tar"), f"calls: {calls}")
    _check(calls[0][3] is None, f"streamed tar should not set content-length: {calls}")
    _check(
        calls[0][4] == "chunked",
        f"streamed tar should use transfer-encoding chunked: {calls}",
    )
    print("ok: copy_from_local dir streams direct tar upload ->", calls[0][0:5])


def test_copy_to_local_dir_uses_direct_tar_download():
    calls = []
    tar_buf = io.BytesIO()
    with tarfile.open(fileobj=tar_buf, mode="w") as tar:
        data = b"from-tar"
        info = tarfile.TarInfo("nested.txt")
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))
    tar_bytes = tar_buf.getvalue()

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, request.url.path, request.url.params.get("path"), request.url.params.get("type")))
        if request.url.path.endswith("/invoke"):
            return httpx.Response(
                200,
                json={"name": "remote-dir", "path": "/tmp/remote-dir", "type": "dir", "size": 0, "permissions": "", "modified_time": 0},
            )
        _check(request.url.path == "/direct/sandbox-demo/download", f"dir download path: {request.url.path}")
        return httpx.Response(200, content=tar_bytes)

    c = _make_client(handler)
    with tempfile.TemporaryDirectory() as d:
        Filesystem(c, "sandbox-demo").copy_to_local("/tmp/remote-dir", d)
        with open(os.path.join(d, "nested.txt"), "rb") as f:
            _check(f.read() == b"from-tar", "tar download extract mismatch")

    _check(calls[0][1] == "/direct/sandbox-demo/invoke", f"stat call: {calls}")
    _check(calls[1] == ("GET", "/direct/sandbox-demo/download", "/tmp/remote-dir", "tar"), f"download call: {calls}")
    print("ok: copy_to_local dir uses direct tar download ->", calls)


def test_direct_invoke_sends_request_id_header_and_body():
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["header"] = request.headers.get("x-yr-request-id")
        seen["body"] = json.loads(request.content.decode())
        return httpx.Response(200, json={"ok": True})

    c = _make_client(handler)
    out = c.invoke("sandbox-demo", "file.exists", {"path": "/"})
    _check(out == {"ok": True}, f"direct invoke result: {out}")
    _check(seen["path"] == "/direct/sandbox-demo/invoke", f"path: {seen}")
    _check(seen["header"] and seen["header"].startswith("invoke-"), f"header: {seen}")
    _check(seen["body"].get("requestId") == seen["header"], f"request id mismatch: {seen}")
    print("ok: direct invoke carries requestId")


def test_resumable_download_continues_from_part_file():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.url.path, request.headers.get("range")))
        _check(request.url.path == "/direct/sandbox-demo/download", f"download path: {request.url.path}")
        _check(request.headers.get("range") == "bytes=4-", f"range header: {request.headers}")
        return httpx.Response(206, content=b"ef", headers={"Content-Range": "bytes 4-5/6"})

    c = _make_client(handler)
    with tempfile.TemporaryDirectory() as d:
        target = os.path.join(d, "out.bin")
        with open(target + ".part", "wb") as f:
            f.write(b"abcd")
        c.download_file_direct("sandbox-demo", "/tmp/out.bin", target)
        with open(target, "rb") as f:
            _check(f.read() == b"abcdef", "resumed download mismatch")
    _check(calls == [("/direct/sandbox-demo/download", "bytes=4-")], f"calls: {calls}")
    print("ok: resumable download continues from .part")


def test_tunnel_client_keeps_http_req_dedup_cache():
    import yr_sandbox.tunnel_client as tunnel_client

    source = inspect.getsource(tunnel_client.TunnelClient._proxy_loop)
    _check("inflight" in source and "completed" in source, "TunnelClient should cache in-flight/completed http_req frames")
    _check("if rid and rid in completed" in source, "TunnelClient should replay completed http_req ids")
    _check("if rid and rid in inflight" in source, "TunnelClient should coalesce running http_req ids")
    print("ok: TunnelClient has http_req id dedup/replay cache")


def test_reverse_tunnel_url_uses_gateway_tunnel_alias():
    import yr_sandbox.sandbox_api as sandbox_api
    import yr_sandbox.tunnel_client as tunnel_client

    os.environ["YR_SERVER_ADDRESS"] = "frontend:8888"
    os.environ["YR_GATEWAY_ADDRESS"] = "router:8080"
    os.environ.pop("YR_GATEWAY_TLS", None)

    seen = {}
    original_client = sandbox_api.SandboxClient
    original_tunnel = tunnel_client.TunnelClient

    class FakeClient:
        direct_enabled = True
        rrt_port = 50090
        token = "test-token"

        def create(self, body):
            seen["create_ports"] = body.get("ports")
            seen["create_env"] = body.get("env", {})
            seen["create_tunnel"] = body.get("tunnel")
            return "sandbox-demo"

        def invoke(self, *args, **kwargs):
            return {"exists": True}

        def delete(self, sandbox_id):
            seen["deleted"] = sandbox_id

        def close(self):
            seen["closed"] = True

        @staticmethod
        def _safe_id(sandbox_id):
            return original_client._safe_id(sandbox_id)

    class FakeTunnelClient:
        def __init__(self, upstream, token=None):
            seen["upstream"] = upstream
            seen["token"] = token

        def start(self, url, timeout=60):
            seen["url"] = url
            seen["timeout"] = timeout
            return True

        def stop(self):
            seen["stopped"] = True

    try:
        sandbox_api.SandboxClient = FakeClient
        tunnel_client.TunnelClient = FakeTunnelClient
        sb = sandbox_api.Sandbox(
            image="python:3.12-slim",
            upstream="127.0.0.1:8000",
            tunnel_connect_timeout=1,
            detached=True,
        )
        _check(sb.get_tunnel_url() == "http://127.0.0.1:8766", "sandbox-side tunnel URL mismatch")
        sb.kill()
    finally:
        sandbox_api.SandboxClient = original_client
        tunnel_client.TunnelClient = original_tunnel
        os.environ.pop("YR_GATEWAY_ADDRESS", None)
        os.environ.pop("YR_GATEWAY_TLS", None)

    _check(
        seen["url"] == "ws://router:8080/tunnel/sandbox-demo",
        f"tunnel URL should use gateway /tunnel alias, got {seen['url']}",
    )
    _check("8765" not in seen["url"], f"tunnel URL leaked control port: {seen['url']}")
    _check(seen["token"] is None, "plaintext tunnel should not carry token by default")
    _check(
        seen["create_ports"] is None,
        f"SDK should not request RRT/tunnel control ports, got {seen['create_ports']}",
    )
    _check(
        seen["create_tunnel"] == {"enabled": True},
        f"SDK should ask frontend for tunnel declaratively, got {seen.get('create_tunnel')}",
    )
    _check(
        "RRT_TUNNEL_WS_PORT" not in seen["create_env"]
        and "RRT_TUNNEL_HTTP_PORT" not in seen["create_env"],
        f"SDK must not set RRT tunnel envs, got {seen['create_env']}",
    )
    print("ok: reverse tunnel URL uses gateway alias and hides control port ->", seen["url"])



def test_reverse_tunnel_uses_frontend_returned_tunnel_metadata():
    import yr_sandbox.sandbox_api as sandbox_api
    import yr_sandbox.tunnel_client as tunnel_client

    os.environ["YR_SERVER_ADDRESS"] = "frontend:8888"
    os.environ["YR_GATEWAY_ADDRESS"] = "router:8080"
    os.environ["YR_GATEWAY_TLS"] = "0"

    seen = {}
    original_client = sandbox_api.SandboxClient
    original_tunnel = tunnel_client.TunnelClient

    class FakeClient:
        direct_enabled = True
        rrt_port = 50090
        token = "test-token"

        def create_info(self, body):
            seen["create_ports"] = body.get("ports")
            seen["create_env"] = body.get("env", {})
            seen["create_tunnel"] = body.get("tunnel")
            return {
                "sandboxId": "sandbox-demo",
                "tunnel": {
                    "url": "/tunnel/frontend-returned",
                    "proxyUrl": "http://127.0.0.1:8766",
                },
            }

        def delete(self, sandbox_id):
            seen["deleted"] = sandbox_id

        def close(self):
            seen["closed"] = True

        @staticmethod
        def _safe_id(sandbox_id):
            return original_client._safe_id(sandbox_id)

    class FakeTunnelClient:
        def __init__(self, upstream, token=None):
            seen["token"] = token

        def start(self, url, timeout=60):
            seen["url"] = url
            return True

        def stop(self):
            pass

    try:
        sandbox_api.SandboxClient = FakeClient
        tunnel_client.TunnelClient = FakeTunnelClient
        sb = sandbox_api.Sandbox(
            image="python:3.12-slim",
            upstream="127.0.0.1:8000",
            proxy_port=9876,
            env={"USER_ENV": "ok"},
            tunnel_connect_timeout=1,
            detached=True,
        )
        _check(sb.get_tunnel_url() == "http://127.0.0.1:8766", "frontend proxyUrl mismatch")
        sb.kill()
    finally:
        sandbox_api.SandboxClient = original_client
        tunnel_client.TunnelClient = original_tunnel
        os.environ.pop("YR_GATEWAY_ADDRESS", None)
        os.environ.pop("YR_GATEWAY_TLS", None)

    _check(seen["url"] == "ws://router:8080/tunnel/frontend-returned", f"returned tunnel url mismatch: {seen['url']}")
    _check(seen["create_ports"] is None, f"SDK leaked control ports: {seen['create_ports']}")
    _check(seen["create_env"] == {"USER_ENV": "ok"}, f"SDK should not set RRT envs: {seen['create_env']}")
    _check(seen["create_tunnel"] == {"enabled": True}, f"declarative tunnel mismatch: {seen['create_tunnel']}")
    _check(seen["token"] is None, "plaintext tunnel should not carry token")
    print("ok: reverse tunnel uses frontend-returned metadata")


def test_reverse_tunnel_example_local_server_serves_owned_ephemeral_port():
    example_path = Path(__file__).resolve().parents[1] / "examples" / "reverse_tunnel.py"
    spec = importlib.util.spec_from_file_location("reverse_tunnel_example", example_path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"cannot load reverse_tunnel example from {example_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    server, thread, temp_dir, port = module.start_local_server(0)
    try:
        _check(port > 0, f"expected ephemeral port, got {port}")
        health = urllib.request.urlopen(
            f"http://127.0.0.1:{port}/health", timeout=2
        ).read().decode()
        index = urllib.request.urlopen(
            f"http://127.0.0.1:{port}/index.html", timeout=2
        ).read().decode()
        _check(health == "OK", f"health body mismatch: {health!r}")
        _check("Hello from local machine!" in index, "index body mismatch")
        print("ok: reverse_tunnel local server uses owned ephemeral port ->", port)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)


def test_tunnel_large_response_example_local_server_serves_owned_ephemeral_port():
    example_path = Path(__file__).resolve().parents[1] / "examples" / "tunnel_large_response.py"
    spec = importlib.util.spec_from_file_location("tunnel_large_response_example", example_path)
    if spec is None or spec.loader is None:
        raise AssertionError(f"cannot load tunnel_large_response example from {example_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    server, thread, temp_dir, file_hashes, port = module.start_local_server(0)
    try:
        _check(port > 0, f"expected ephemeral port, got {port}")
        health = urllib.request.urlopen(
            f"http://127.0.0.1:{port}/health", timeout=2
        ).read()
        first_label, first_size = module.TEST_SIZES[0]
        data = urllib.request.urlopen(
            f"http://127.0.0.1:{port}/{first_label}.bin", timeout=2
        ).read()
        _check(health == b"OK", f"health body mismatch: {health!r}")
        _check(len(data) == first_size, f"large fixture size mismatch: {len(data)}")
        _check(
            hashlib.sha256(data).hexdigest() == file_hashes[first_label],
            "large fixture hash mismatch",
        )
        print("ok: tunnel_large_response local server uses owned ephemeral port ->", port)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)


def test_tunnel_client_ignores_proxy_env_for_local_upstream():
    import yr_sandbox.tunnel_client as tunnel_client

    source = inspect.getsource(tunnel_client.TunnelClient._proxy_loop)
    _check(
        "trust_env=False" in source,
        "TunnelClient upstream HTTP client must ignore HTTP_PROXY/NO_PROXY env",
    )
    print("ok: TunnelClient ignores proxy env for local upstream")


def test_safe_id_matches_router_sanitize():
    _check(
        SandboxClient._safe_id("sandbox@v1/foo.bar_baz") == "sandbox-at-v1-foo-bar-baz",
        "safe_id sanitize mismatch",
    )
    _check(
        SandboxClient._safe_id("default-rtt") == "default-rtt", "safe_id passthrough"
    )
    print("ok: safe_id sanitize")


if __name__ == "__main__":
    test_create_uses_sse_and_returns_running_final()
    test_create_rejects_timeout_final()
    test_create_rejects_stream_without_final()
    test_sandbox_create_timeout_precedence_and_body()
    test_sandbox_create_timeout_validation()
    test_direct_success_no_fallback()
    test_direct_5xx_does_not_retry_current_invoke_and_sticks()
    test_direct_read_timeout_does_not_retry_unknown_outcome()
    test_direct_4xx_does_not_fallback()
    test_direct_invalid_json_does_not_retry_unknown_outcome()
    test_direct_connect_error_falls_back()
    test_direct_fallback_when_frontend_direct_missing()
    test_direct_binary_upload_success()
    test_files_write_bytes_uses_direct_upload()
    test_files_write_text_uses_direct_upload()
    test_copy_from_local_small_file_uses_single_direct_upload()
    test_copy_from_local_file_uses_resumable_direct_upload_above_threshold()
    test_files_read_uses_direct_download()
    test_copy_to_local_file_uses_direct_download()
    test_direct_invoke_sends_request_id_header_and_body()
    test_resumable_download_continues_from_part_file()
    test_tunnel_client_keeps_http_req_dedup_cache()
    test_copy_from_local_dir_streams_direct_tar_upload()
    test_copy_to_local_dir_uses_direct_tar_download()
    test_reverse_tunnel_url_uses_gateway_tunnel_alias()
    test_reverse_tunnel_uses_frontend_returned_tunnel_metadata()
    test_reverse_tunnel_example_local_server_serves_owned_ephemeral_port()
    test_tunnel_large_response_example_local_server_serves_owned_ephemeral_port()
    test_tunnel_client_ignores_proxy_env_for_local_upstream()
    test_safe_id_matches_router_sanitize()
    print("\nALL PASS")
