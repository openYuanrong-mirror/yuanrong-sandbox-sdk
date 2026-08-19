"""Sandbox API for openYuanrong, backed by frontend sandbox v1 and RRT.

Sandbox lifecycle is server-side and reached through the frontend HTTP control
plane. Commands, filesystem operations, shell sessions, direct file transfer,
and reverse tunnel helpers are exposed as Python objects on ``Sandbox``.
"""

import logging
import os
import re
from collections.abc import Mapping
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse

from ._transport import SandboxClient
from .commands import Commands
from .filesystem import Filesystem
from .pty import Pty
from .shell import Shells
from .types import (
    ConnectionConfig,
    Mount,
    NetworkPolicy,
    PortForwarding,
    S3Config,
    SandboxInfo,
)

logger = logging.getLogger(__name__)

DEFAULT_CREATE_TIMEOUT = 60
SCHEDULE_TIMEOUT_BUFFER = 30
_AFFINITY_KIND_RESOURCE = 0
_AFFINITY_REQUIRED = 2
_LABEL_OPERATION_IN = 0
_NODE_ID_LABEL = "NODE_ID"
_SUPPORTED_XPU_TYPES = frozenset({"gpu"})


def _get_create_timeout(timeout: Optional[int]) -> int:
    if timeout is not None:
        value = timeout
    else:
        raw = os.environ.get(
            "YR_SANDBOX_CREATE_TIMEOUT", str(DEFAULT_CREATE_TIMEOUT)
        ).strip()
        try:
            value = int(raw)
        except ValueError as exc:
            raise ValueError(
                "YR_SANDBOX_CREATE_TIMEOUT must be an integer number of seconds"
            ) from exc
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("create_timeout must be a positive integer")
    return value


def _resolve_create_timeouts(
    create_timeout: Optional[int], schedule_timeout: Optional[int]
) -> tuple[int, int]:
    if schedule_timeout is not None and (
        isinstance(schedule_timeout, bool)
        or not isinstance(schedule_timeout, int)
        or schedule_timeout <= 0
    ):
        raise ValueError("schedule_timeout must be a positive integer")

    resolved_create = _get_create_timeout(create_timeout)
    if schedule_timeout is None:
        schedule_timeout = 30

    if schedule_timeout > resolved_create:
        raise ValueError(
            "schedule_timeout must be less than or equal to create_timeout"
        )
    if resolved_create - schedule_timeout < SCHEDULE_TIMEOUT_BUFFER:
        raise ValueError(
            "create_timeout - schedule_timeout must be at least "
            f"{SCHEDULE_TIMEOUT_BUFFER}"
        )
    return resolved_create, schedule_timeout


def _get_tunnel_connect_timeout(timeout: Optional[float]) -> float:
    if timeout is not None:
        value = float(timeout)
    else:
        raw = os.environ.get("YR_TUNNEL_CONNECT_TIMEOUT", "60")
        try:
            value = float(raw)
        except ValueError as e:
            raise ValueError(
                "YR_TUNNEL_CONNECT_TIMEOUT must be a number of seconds"
            ) from e
    if value <= 0:
        raise ValueError("tunnel_connect_timeout must be greater than 0")
    return value


def _compose_gateway_url(*, gateway: str, scheme: str, path: str) -> str:
    """Compose a gateway URL from a frontend-returned path or URL.

    Frontend normally returns a path-only tunnel URL so deployments can choose
    the external gateway address locally. If the frontend returns a full URL,
    keep only its path; the SDK still owns the public gateway host and
    ws/wss scheme selection via YR_GATEWAY_ADDRESS/YR_GATEWAY_TLS.
    """
    if not gateway:
        raise ValueError("YR_GATEWAY_ADDRESS or YR_SERVER_ADDRESS must be set")
    parsed = urlparse(path)
    route = parsed.path or path
    if parsed.query:
        route = f"{route}?{parsed.query}"
    if not route.startswith("/"):
        route = f"/{route}"
    return f"{scheme}://{gateway}{route}"


def _gateway_address(connection: Optional[ConnectionConfig]) -> str:
    if connection is not None:
        return connection.gateway_address or connection.server_address
    gateway = os.environ.get("YR_GATEWAY_ADDRESS", "").strip()
    if not gateway:
        gateway = os.environ.get("YR_SERVER_ADDRESS", "").strip()
    if not gateway:
        raise ValueError("YR_GATEWAY_ADDRESS or YR_SERVER_ADDRESS must be set")
    return gateway


def _gateway_uses_tls(connection: Optional[ConnectionConfig]) -> bool:
    if connection is not None:
        return connection.gateway_use_tls
    return os.environ.get("YR_GATEWAY_TLS", "0").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def _validate_xpu(xpu: Optional[str]) -> None:
    if xpu is None:
        return
    if not isinstance(xpu, str):
        raise TypeError("xpu must be a string or None")

    fields = xpu.split(":")
    if len(fields) != 3 or not fields[0] or not fields[2]:
        raise ValueError("xpu must have exactly three fields: type:model:count")
    if any(field != field.strip() for field in fields):
        raise ValueError("xpu fields must not contain surrounding whitespace")

    xpu_type, _, count_text = fields
    xpu_type = xpu_type.lower()
    if xpu_type not in _SUPPORTED_XPU_TYPES:
        raise ValueError(f"unsupported xpu type: {xpu_type}")
    if re.fullmatch(r"[0-9]+", count_text) is None:
        raise ValueError("xpu count must be a positive integer")
    count = int(count_text)
    if count <= 0:
        raise ValueError("xpu count must be a positive integer")


class Sandbox:
    """High-level sandbox API for openYuanrong sandboxes.

    Usage::

        with Sandbox(image="python:3.12-slim", cpu=2000, memory=4096) as sb:
            sb.files.write("/tmp/hello.txt", "hello world")
            result = sb.commands.run("cat /tmp/hello.txt")
            print(result.stdout)

            sh = await sb.shells.create(cwd="/tmp")
            await sh.run("export FOO=bar")
            result = await sh.run("echo $FOO")  # → bar
    """

    def __init__(
        self,
        image: Optional[str] = None,
        rootfs: Optional[S3Config] = None,
        runtime: str = "runsc",
        cpu: int = 1000,
        memory: int = 4096,
        cpu_limit: int = 0,
        mem_limit: int = 0,
        idle_timeout: int = 300,
        schedule_timeout: int = 30,
        env: Optional[Dict[str, str]] = None,
        name: Optional[str] = None,
        cwd: Optional[str] = None,
        port_forwardings: Optional[List[Union[int, PortForwarding]]] = None,
        mounts: Optional[List[Mount]] = None,
        upstream: Optional[str] = None,
        proxy_port: int = 8766,
        tunnel_connect_timeout: Optional[float] = None,
        detached: bool = False,
        node_id: Optional[str] = None,
        *,
        xpu: Optional[str] = None,
        storage_mb: Optional[int] = None,
        storage_limit_mb: int = 0,
        network: Optional[NetworkPolicy] = None,
        create_timeout: Optional[int] = None,
        connection: Optional[ConnectionConfig] = None,
        extra_config: Optional[Dict[str, Any]] = None,
    ):
        """Create a new sandbox.

        Args:
            image: Container image to use (e.g. ``"python:3.12-slim"``).
            rootfs: S3-compatible EROFS root filesystem configuration.
            runtime: Sandbox isolation runtime identifier. Defaults to
                ``runsc`` and is validated by the runtime layer.
            cpu: CPU scheduling request in milli-cores (default 1000).
            memory: Memory scheduling request in MB (default 4096).
            cpu_limit: CPU cgroup limit in milli-cores (0 = same as *cpu*).
            mem_limit: Memory cgroup limit in MB (0 = same as *memory*).
            idle_timeout: Seconds before idle sandbox is reclaimed (default 300).
            create_timeout: Logical create budget in seconds. Defaults to
                ``YR_SANDBOX_CREATE_TIMEOUT`` or 60 seconds.
            schedule_timeout: Scheduling budget in seconds (default 30).
            env: Environment variables to set in the sandbox.
            name: Logical name for the sandbox instance.
            cwd: Working directory inside the sandbox.
            port_forwardings: Ports to forward from the sandbox. Each entry is
                a port number (defaults to TCP) or a ``PortForwarding`` object.
            mounts: Custom mount specifications for the sandbox.
            upstream: ``host:port`` or HTTP(S) URL of an SDK-side service to
                expose inside the sandbox.
            proxy_port: Sandbox-side HTTP proxy port for ``upstream``. The
                frontend derives the WebSocket tunnel port as
                ``proxy_port - 1``. Defaults to 8766.
            tunnel_connect_timeout: Seconds to wait for the tunnel WebSocket.
            detached: If True, ``kill()`` / context-manager exit skips teardown.
            xpu: Optional whole-device XPU request in ``type:model:count``
                format. Leave ``model`` empty to accept any model. The first
                version supports one ``gpu`` request.
            storage_mb: Temporary writable root filesystem capacity in MiB.
                ``None`` uses the cluster default.
            storage_limit_mb: Writable root filesystem hard limit in MiB. ``0``
                uses *storage_mb* (or the cluster default when *storage_mb* is
                omitted).
            network: Optional creation-time network policy. Omitting it allows
                unrestricted network access.
            connection: Explicit frontend and gateway connection settings.
                Omitting it reads the existing ``YR_*`` environment variables.
            extra_config: Extra sandbox-side configuration forwarded to sandboxd.
        """
        if image is not None and (
            not isinstance(image, str) or not image.strip()
        ):
            raise ValueError("image must be a non-empty string")
        if rootfs is not None and not isinstance(rootfs, S3Config):
            raise TypeError("rootfs must be an S3Config")
        if image is not None and rootfs is not None:
            raise ValueError("image and rootfs are mutually exclusive")
        if env is not None and (
            not isinstance(env, Mapping)
            or not all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in env.items()
            )
        ):
            raise TypeError("env must map strings to strings")
        if name is not None and (
            not isinstance(name, str) or not name.strip()
        ):
            raise ValueError("name must be a non-empty string")
        if cwd is not None:
            if not isinstance(cwd, str):
                raise TypeError("cwd must be a string")
            if not cwd.startswith("/"):
                raise ValueError("cwd must be an absolute POSIX path")
        if not isinstance(detached, bool):
            raise TypeError("detached must be a boolean")
        if node_id is not None:
            if not isinstance(node_id, str):
                raise TypeError("node_id must be a string")
            if not node_id:
                raise ValueError("node_id cannot be empty string")
        _validate_xpu(xpu)
        if storage_mb is not None:
            if isinstance(storage_mb, bool) or not isinstance(storage_mb, int):
                raise TypeError("storage_mb must be an integer or None")
            if storage_mb <= 0:
                raise ValueError("storage_mb must be greater than 0")
        if isinstance(storage_limit_mb, bool) or not isinstance(
            storage_limit_mb, int
        ):
            raise TypeError("storage_limit_mb must be an integer")
        if storage_limit_mb < 0:
            raise ValueError("storage_limit_mb must be 0 or greater")
        if storage_mb is not None and 0 < storage_limit_mb < storage_mb:
            raise ValueError(
                "storage_limit_mb must be greater than or equal to storage_mb"
            )
        if network is not None and not isinstance(network, NetworkPolicy):
            raise TypeError("network must be a NetworkPolicy or None")
        if connection is not None and not isinstance(connection, ConnectionConfig):
            raise TypeError("connection must be a ConnectionConfig or None")
        if mounts is None:
            mount_list: List[Mount] = []
        else:
            if isinstance(mounts, (str, bytes)):
                raise TypeError("mounts must be a sequence of Mount objects")
            mount_list = list(mounts)
            if not all(isinstance(mount, Mount) for mount in mount_list):
                raise TypeError("mounts must contain only Mount objects")
        if upstream is not None and (
            not isinstance(upstream, str) or not upstream.strip()
        ):
            raise ValueError("upstream must be a non-empty address")
        if isinstance(proxy_port, bool) or not isinstance(proxy_port, int):
            raise TypeError("proxy_port must be an integer")
        if not 2 <= proxy_port <= 65535:
            raise ValueError("proxy_port must be between 2 and 65535")

        # ── port_forwardings ──────────────────────────────────────────────
        self._forwarded_ports: set = set()
        pf_ports: List[str] = []
        if port_forwardings:
            if isinstance(port_forwardings, (str, bytes)):
                raise TypeError(
                    "port_forwardings must contain integers or PortForwarding objects"
                )
            ports: List[int] = []
            for forwarding in port_forwardings:
                if isinstance(forwarding, PortForwarding):
                    port = forwarding.port
                else:
                    port = forwarding
                if isinstance(port, bool) or not isinstance(port, int):
                    raise TypeError(
                        "forwarded port must be an integer or PortForwarding object"
                    )
                if not 1 <= port <= 65535:
                    raise ValueError("forwarded port must be between 1 and 65535")
                ports.append(port)
            if len(set(ports)) != len(ports):
                raise ValueError("port_forwardings must not contain duplicate ports")
            self._forwarded_ports.update(ports)
            pf_ports.extend(str(port) for port in ports)
        if upstream is not None:
            conflicts = self._forwarded_ports.intersection(
                {proxy_port - 1, proxy_port}
            )
            if conflicts:
                rendered = ", ".join(str(port) for port in sorted(conflicts))
                raise ValueError(
                    "reverse tunnel ports conflict with port_forwardings: "
                    f"{rendered}"
                )

        # ── reverse tunnel ────────────────────────────────────────────────
        self._tunnel_client = None
        self._tunnel_url = f"http://127.0.0.1:{proxy_port}"
        self._closed = False
        self._upstream = upstream

        # ── build create body ─────────────────────────────────────────────
        resolved_create_timeout, resolved_schedule_timeout = _resolve_create_timeouts(
            create_timeout, schedule_timeout
        )
        body: Dict[str, Any] = {
            "namespace": "default",
            "idleTimeoutSeconds": idle_timeout,
            "createTimeoutSeconds": resolved_create_timeout,
            "scheduleTimeoutSeconds": resolved_schedule_timeout,
            "rootfs": {"runtime": runtime},
        }
        if image:
            body["rootfs"].update(
                {
                    "type": "image",
                    "readonly": False,
                    "imageurl": image,
                }
            )
        elif rootfs:
            body["rootfs"].update(
                {
                    "type": "s3",
                    "storageInfo": rootfs.to_dict(),
                }
            )
        if name:
            body["name"] = name
        body["cpu"] = cpu
        body["memory"] = memory
        body["cpu_limit"] = cpu_limit
        body["mem_limit"] = mem_limit
        if xpu is not None:
            body["xpu"] = xpu
        if storage_mb is not None:
            body["storageMb"] = storage_mb
        body["storage_limit_mb"] = storage_limit_mb
        if env:
            body["env"] = dict(env)
        if node_id:
            body["scheduleAffinities"] = [
                {
                    "kind": _AFFINITY_KIND_RESOURCE,
                    "affinity": _AFFINITY_REQUIRED,
                    "labelOps": [
                        {
                            "type": _LABEL_OPERATION_IN,
                            "labelKey": _NODE_ID_LABEL,
                            "labelValues": [node_id],
                        }
                    ],
                }
            ]
        if mount_list:
            body["mounts"] = [mount.to_dict() for mount in mount_list]
        if network is not None and not network.is_empty:
            body["network"] = network.to_dict()
        if extra_config:
            body["extra_config"] = dict(extra_config)
        if detached:
            body["lifecycle"] = "detached"
        if upstream is not None:
            # Frontend derives the WebSocket port as proxyPort - 1, owns both
            # forwarded ports and RRT_TUNNEL_* env injection, then returns a
            # stable /tunnel/{safeID} URL path.
            body["tunnel"] = {"enabled": True, "proxyPort": proxy_port}

        self._detached = detached
        self._image = image
        self._cpu = cpu
        self._memory = memory
        self._cwd = cwd

        # ── ports: user port_forwardings only ─────────────────────────────
        # Frontend owns RRT_HTTP_PORT=50090 and its sandbox network mapping for
        # /direct. SDK callers should not expose that internal control port.
        if connection is None:
            self._client = SandboxClient()
        else:
            self._client = SandboxClient(connection=connection)
        self._connection = connection
        if pf_ports:
            body["ports"] = pf_ports

        self._sid = ""
        try:
            create_info = self._create(body)
            sandbox_id = create_info.get("sandboxId") or create_info.get("instanceId")
            if not isinstance(sandbox_id, str) or not sandbox_id:
                raise RuntimeError(
                    f"create response missing sandbox id: {create_info}"
                )
            self._sid = sandbox_id

            # ── reverse tunnel: connect after sandbox is running ──────────
            if upstream is not None:
                # Build the tunnel WebSocket URL via the sandbox gateway.
                # The route owns the internal tunnel control-port mapping.
                gateway = _gateway_address(self._connection)

                tunnel_info = create_info.get("tunnel") or {}
                if not isinstance(tunnel_info, dict):
                    tunnel_info = {}
                self._tunnel_url = (
                    tunnel_info.get("proxyUrl")
                    or f"http://127.0.0.1:{proxy_port}"
                )
                tunnel_url = tunnel_info.get("url") or tunnel_info.get("path")
                safe_id = self._client._safe_id(self._sid)
                tls = _gateway_uses_tls(self._connection)
                ws_scheme = "wss" if tls else "ws"
                tunnel_ws_url = _compose_gateway_url(
                    gateway=gateway,
                    scheme=ws_scheme,
                    path=tunnel_url or f"/tunnel/{safe_id}",
                )
                connect_timeout = _get_tunnel_connect_timeout(
                    tunnel_connect_timeout
                )

                from .tunnel_client import TunnelClient

                # Only carry the JWT over a TLS tunnel. Plaintext mode is for
                # auth-disabled local/dev frontends.
                tunnel_token = self._client.token if tls else None
                self._tunnel_client = TunnelClient(upstream, token=tunnel_token)
                logger.info(
                    "Starting TunnelClient: sandbox_id=%s name=%s url=%s "
                    "timeout=%.1fs",
                    safe_id,
                    name or "",
                    tunnel_ws_url,
                    connect_timeout,
                )
                if self._tunnel_client.start(
                    tunnel_ws_url, timeout=connect_timeout
                ):
                    logger.info(
                        "TunnelClient connected: sandbox_id=%s name=%s",
                        safe_id,
                        name or "",
                    )
                else:
                    self._tunnel_client.stop()
                    self._tunnel_client = None
                    raise RuntimeError(
                        "TunnelClient connection timeout after "
                        f"{connect_timeout:.1f}s: sandbox_id={safe_id} "
                        f"name={name or ''} url={tunnel_ws_url}. "
                        "The tunnel route may be missing or not ready."
                    )

            self._files = Filesystem(self._client, self._sid)
            self._commands = Commands(self._client, self._sid, default_cwd=self._cwd)
            self._shells = Shells(self._client, self._sid, default_cwd=self._cwd)
            self._pty = Pty(self._sid, connection=self._connection)
        except Exception:
            if self._tunnel_client is not None:
                try:
                    self._tunnel_client.stop()
                except Exception as cleanup_error:
                    logger.warning(
                        "tunnel rollback failed: sandbox_id=%s error=%s",
                        self._sid,
                        cleanup_error,
                    )
                self._tunnel_client = None
            try:
                if self._sid:
                    self._client.delete(self._sid)
            except Exception as cleanup_error:
                logger.warning(
                    "sandbox rollback failed: sandbox_id=%s error=%s",
                    self._sid,
                    cleanup_error,
                )
            try:
                self._client.close()
            except Exception as cleanup_error:
                logger.warning(
                    "client rollback failed: sandbox_id=%s error=%s",
                    self._sid,
                    cleanup_error,
                )
            self._closed = True
            raise

    # ── sub-resources ──────────────────────────────────────────────────

    @property
    def files(self):
        return self._files

    @property
    def commands(self):
        return self._commands

    @property
    def shells(self):
        return self._shells

    @property
    def pty(self):
        return self._pty

    @property
    def id(self) -> str:
        """Sandbox ID assigned by the frontend."""
        return self._sid

    @property
    def sandbox_id(self) -> str:
        return self._sid

    # ── port forwarding ─────────────────────────────────────────────────

    def get_port_url(self, port: int) -> str:
        """Return the external URL to reach a forwarded port.

        URL format: ``http://{gateway}/{sandbox_id}/{port}``.
        """
        if port not in self._forwarded_ports:
            raise ValueError(
                f"Port {port} is not in forwarded ports: {self._forwarded_ports}"
            )
        gateway = _gateway_address(self._connection)
        safe_id = self._client._safe_id(self._sid)
        return f"http://{gateway}/{safe_id}/{port}"

    # ── reverse tunnel ──────────────────────────────────────────────────

    def get_tunnel_url(self) -> str:
        """Return the internal HTTP proxy URL for sandbox code.

        Returns:
            str: e.g. "http://127.0.0.1:8766"
        Raises:
            RuntimeError: if no reverse tunnel was configured.
        """
        if self._upstream is None:
            raise RuntimeError(
                "No upstream configured. Pass upstream= to Sandbox()."
            )
        return self._tunnel_url

    def _create(self, body: Dict[str, Any]) -> Dict[str, Any]:
        create_info = getattr(self._client, "create_info", None)
        if callable(create_info):
            return create_info(body)
        sid = self._client.create(body)
        return getattr(self._client, "last_create", None) or {"sandboxId": sid}

    # ── lifecycle ──────────────────────────────────────────────────────

    def is_running(self) -> bool:
        if self._closed:
            return False
        try:
            info = self._client.instance_info(self._sid)
            return info.get("status") == "running"
        except Exception:
            return False

    def get_info(self) -> SandboxInfo:
        info = self._client.instance_info(self._sid)
        return SandboxInfo(
            id=str(info.get("id") or self._sid),
            state=str(info.get("status") or "stopped"),
            cpu=info.get("required_cpu", self._cpu),
            memory=info.get("required_mem", self._memory),
            image=info.get("image", self._image),
        )

    def _close(self, *, delete_remote: bool) -> None:
        if self._closed:
            return
        self._closed = True
        if self._tunnel_client is not None:
            try:
                self._tunnel_client.stop()
            except Exception as e:
                logger.debug("tunnel cleanup during close failed: %s", e)
            self._tunnel_client = None
        try:
            self._shells.close()
        except Exception as e:
            logger.debug("shell cleanup during close failed: %s", e)
        try:
            self._pty._close()
        except Exception as e:
            logger.debug("PTY cleanup during close failed: %s", e)
        try:
            if delete_remote and not self._detached:
                self._client.delete(self._sid)
        finally:
            self._client.close()

    def close(self) -> None:
        """Release local clients without deleting the remote sandbox.

        This operation is idempotent. Use :meth:`kill` when the same handle
        should also delete a non-detached remote sandbox.
        """

        self._close(delete_remote=False)

    def kill(self) -> None:
        """Release local clients and delete a non-detached remote sandbox."""

        self._close(delete_remote=True)

    @classmethod
    def delete(
        cls,
        sandbox_id: str,
        *,
        connection: Optional[ConnectionConfig] = None,
    ) -> None:
        if connection is not None and not isinstance(connection, ConnectionConfig):
            raise TypeError("connection must be a ConnectionConfig or None")
        if connection is None:
            client = SandboxClient()
        else:
            client = SandboxClient(connection=connection)
        try:
            client.delete(sandbox_id)
        finally:
            client.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.kill()

    def __del__(self):
        try:
            self.kill()
        except Exception:
            pass
