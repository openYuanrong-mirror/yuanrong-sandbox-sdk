"""TunnelClient: WebSocket-based reverse tunnel for RRT sandboxes.

Connects to the sandbox's tunnel WebSocket port through the sandbox
router (or frontend gateway) and proxies HTTP and application WebSocket
traffic to services running on the client machine.

Architecture:
  [Local Machine]
    upstream (e.g. 127.0.0.1:8000)
         ^ HTTP
    TunnelClient (control WebSocket + HTTP/WebSocket proxy)
         | WS via router/gateway
         v
  [Sandbox]
    rrt-runtime tunnel server (Port A:8765 WS, Port B:8766 HTTP)
    sandbox code → http://127.0.0.1:8766 → WS → local upstream
"""

import asyncio
import logging
import os
import threading
import time
from typing import Any, Optional
from urllib.parse import quote, urljoin, urlsplit, urlunsplit

import websockets.asyncio.client as ws_client
import websockets.exceptions as ws_exc

logger = logging.getLogger(__name__)

_RECONNECT_DELAY = 1.0
_COMPLETED_FRAME_TTL = 300.0
_COMPLETED_FRAME_LIMIT = 1024
_WS_CHANNEL_QUEUE_LIMIT = 100


class TunnelClient:
    """WebSocket tunnel from sandbox back to a local upstream service.

    Usage::

        tunnel = TunnelClient(upstream="127.0.0.1:8000")
        tunnel.start("ws://router/safeID/8765", timeout=30)
        # sandbox code can now reach local :8000 via its proxy port
        tunnel.stop()
    """

    def __init__(self, upstream: str, token: Optional[str] = None):
        self._upstream = upstream
        self._token = token
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._connected = threading.Event()
        self._stopping = threading.Event()
        self._ws: Any = None

    def start(self, tunnel_ws_url: str, timeout: float = 60) -> bool:
        """Start the tunnel client in a background thread.

        Returns True if the WebSocket connected within *timeout* seconds.
        """
        self._stopping.clear()
        self._connected.clear()
        self._thread = threading.Thread(
            target=self._run_loop,
            args=(tunnel_ws_url,),
            daemon=True,
            name="tunnel-client",
        )
        self._thread.start()
        return self._connected.wait(timeout=timeout)

    def stop(self) -> None:
        """Signal the tunnel client to stop and wait for the thread."""
        self._stopping.set()
        if self._loop is not None and self._loop.is_running() and self._ws is not None:
            fut = asyncio.run_coroutine_threadsafe(self._ws.close(), self._loop)
            try:
                fut.result(timeout=2)
            except Exception:
                pass
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=5)
        if self._thread is not None and self._thread.is_alive() and self._loop is not None:
            # Last-resort fallback for a wedged event loop. The normal path
            # closes the WebSocket above, letting _connect_loop exit cleanly.
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=2)

    def _run_loop(self, tunnel_ws_url: str) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._connect_loop(tunnel_ws_url))
        except RuntimeError:
            # Event loop was stopped by the last-resort shutdown path.
            pass
        finally:
            pending = [
                task for task in asyncio.all_tasks(self._loop) if not task.done()
            ]
            for task in pending:
                task.cancel()
            if pending:
                self._loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
            self._loop.run_until_complete(self._loop.shutdown_asyncgens())
            self._loop.close()

    async def _connect_loop(self, tunnel_ws_url: str) -> None:
        failures = 0
        while not self._stopping.is_set():
            try:
                import ssl as _ssl

                _ctx = _ssl.create_default_context()
                _ctx.check_hostname = False
                _ctx.verify_mode = _ssl.CERT_NONE
                _extra_headers = {}
                connect_url = tunnel_ws_url
                if self._token:
                    _extra_headers["X-Auth"] = self._token
                    # Keep credentials out of gateway access logs by default.
                    # Operators that sit behind a gateway known to drop custom
                    # WebSocket headers can explicitly enable the query-token
                    # fallback.
                    token_query_fallback = os.environ.get(
                        "YR_TUNNEL_TOKEN_QUERY_FALLBACK", "0"
                    ).strip().lower()
                    if token_query_fallback in ("1", "true", "yes"):
                        sep = "&" if "?" in connect_url else "?"
                        connect_url = (
                            f"{connect_url}{sep}token={quote(self._token, safe='')}"
                        )
                _ssl_ctx = None
                if tunnel_ws_url.startswith("wss://"):
                    _ssl_ctx = _ssl.create_default_context()
                    _ssl_ctx.check_hostname = False
                    _ssl_ctx.verify_mode = _ssl.CERT_NONE
                async with ws_client.connect(
                    connect_url,
                    max_size=2**23,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                    ssl=_ssl_ctx,
                    additional_headers=_extra_headers,
                ) as ws:
                    self._ws = ws
                    self._connected.set()
                    failures = 0
                    logger.info("TunnelClient connected: %s", tunnel_ws_url)
                    try:
                        await self._proxy_loop(ws)
                    finally:
                        self._ws = None
            except (ws_exc.ConnectionClosed, OSError, asyncio.TimeoutError) as e:
                self._connected.clear()
                failures += 1
                if failures == 1 or failures % 10 == 0:
                    logger.warning(
                        "TunnelClient disconnected (attempt %d): %s", failures, e
                    )
                if self._stopping.is_set():
                    return
                await asyncio.sleep(min(_RECONNECT_DELAY * min(failures, 30), 30))
            except Exception as e:
                failures += 1
                logger.error("TunnelClient unexpected error: %s", e)
                if self._stopping.is_set():
                    return
                await asyncio.sleep(_RECONNECT_DELAY)

    async def _proxy_loop(self, ws) -> None:
        """Relay rrt tunnel frames to/from the upstream HTTP service.

        rrt's tunnel server (Port A) speaks a JSON frame protocol over WS TEXT
        messages (see api/rust/rrt-daemon/src/runtime/tunnel.rs ``Frame``): an
        ``http_req`` frame carries a sandbox-side request with a base64 body; we
        fetch the local upstream and answer with an ``http_resp`` frame keyed by
        the same id (base64 body). ``ping`` frames are answered with ``pong``
        (heartbeat). HTTP request frames are handled concurrently so multiple
        sandbox-side requests can share the same tunnel.
        """
        import asyncio
        import base64
        import json

        import httpx

        send_lock = asyncio.Lock()
        inflight: dict = {}
        completed: dict = {}
        ws_channels: dict[str, asyncio.Queue] = {}

        async def send_frame(obj: dict) -> None:
            # websockets does not allow concurrent send() from multiple tasks;
            # serialize since http_req frames are handled on their own tasks.
            async with send_lock:
                await ws.send(json.dumps(obj))

        def cleanup_completed() -> None:
            now = time.monotonic()
            expired = [
                rid
                for rid, (_, ts) in completed.items()
                if now - ts > _COMPLETED_FRAME_TTL
            ]
            for rid in expired:
                completed.pop(rid, None)
            while len(completed) > _COMPLETED_FRAME_LIMIT:
                oldest = min(completed.items(), key=lambda item: item[1][1])[0]
                completed.pop(oldest, None)

        async def build_http_resp_frame(client, frame: dict) -> dict:
            rid = frame.get("id", "")
            method = frame.get("method", "GET")
            path = frame.get("path", "/")
            req_headers = {
                k: v
                for k, v in (frame.get("headers") or {}).items()
                if k.lower() not in ("host", "content-length", "connection")
            }
            try:
                body_b64 = frame.get("body") or ""
                body = base64.b64decode(body_b64) if body_b64 else b""
                base_url = self._upstream
                if "://" not in base_url:
                    base_url = f"http://{base_url}"
                resp = await client.request(
                    method,
                    urljoin(f"{base_url.rstrip('/')}/", path.lstrip("/")),
                    headers=req_headers or None,
                    content=body or None,
                )
                return {
                    "type": "http_resp",
                    "id": rid,
                    "status": resp.status_code,
                    "headers": dict(resp.headers),
                    "body": base64.b64encode(resp.content).decode("ascii"),
                }
            except Exception as e:  # upstream unreachable / fetch error
                logger.debug("tunnel http_req upstream error: %s", e)
                return {"type": "error", "id": rid, "message": str(e)}

        async def await_and_send(rid: str, task) -> None:
            frame = await task
            await send_frame(frame)

        def handle_http_req(client, frame: dict):
            rid = frame.get("id", "")
            cleanup_completed()
            if rid and rid in completed:
                cached, _ = completed[rid]
                return asyncio.ensure_future(send_frame(cached))
            if rid and rid in inflight:
                return asyncio.ensure_future(await_and_send(rid, inflight[rid]))

            task = asyncio.ensure_future(build_http_resp_frame(client, frame))
            if rid:
                inflight[rid] = task

                def done_callback(t, request_id=rid):
                    inflight.pop(request_id, None)
                    if not t.cancelled():
                        try:
                            completed[request_id] = (t.result(), time.monotonic())
                            cleanup_completed()
                        except Exception:
                            pass

                task.add_done_callback(done_callback)
            return asyncio.ensure_future(await_and_send(rid, task))

        def websocket_target(path: str) -> str:
            base_url = self._upstream
            if "://" not in base_url:
                base_url = f"http://{base_url}"
            target = urljoin(f"{base_url.rstrip('/')}/", path.lstrip("/"))
            parsed = urlsplit(target)
            if parsed.scheme == "http":
                scheme = "ws"
            elif parsed.scheme == "https":
                scheme = "wss"
            elif parsed.scheme in ("ws", "wss"):
                scheme = parsed.scheme
            else:
                raise ValueError(
                    f"unsupported reverse WebSocket target scheme: {parsed.scheme}"
                )
            return urlunsplit(parsed._replace(scheme=scheme))

        def websocket_options(frame: dict) -> dict[str, Any]:
            headers = frame.get("headers") or {}
            additional_headers: dict[str, str] = {}
            origin: str | None = None
            subprotocols: list[str] | None = None
            ignored = {
                "connection",
                "content-length",
                "host",
                "sec-websocket-extensions",
                "sec-websocket-key",
                "sec-websocket-version",
                "upgrade",
            }
            for key, value in headers.items():
                lower = key.lower()
                if lower == "origin":
                    origin = value
                elif lower == "sec-websocket-protocol":
                    subprotocols = [
                        item.strip() for item in value.split(",") if item.strip()
                    ]
                elif lower not in ignored:
                    additional_headers[key] = value
            options: dict[str, Any] = {
                "max_size": 2**23,
                "additional_headers": additional_headers or None,
            }
            if origin:
                options["origin"] = origin
            if subprotocols:
                options["subprotocols"] = subprotocols
            return options

        async def handle_ws_connect(frame: dict) -> None:
            rid = frame.get("id", "")
            if not rid:
                return
            if rid in ws_channels:
                await send_frame(
                    {
                        "type": "error",
                        "id": rid,
                        "message": "duplicate WebSocket channel id",
                    }
                )
                return

            queue: asyncio.Queue = asyncio.Queue(maxsize=_WS_CHANNEL_QUEUE_LIMIT)
            ws_channels[rid] = queue
            try:
                async with ws_client.connect(
                    websocket_target(frame.get("path", "/")),
                    **websocket_options(frame),
                ) as upstream_ws:
                    await send_frame({"type": "ws_connected", "id": rid})

                    async def from_upstream() -> None:
                        async for upstream_message in upstream_ws:
                            if isinstance(upstream_message, str):
                                data = upstream_message
                                binary = False
                            else:
                                data = base64.b64encode(upstream_message).decode("ascii")
                                binary = True
                            await send_frame(
                                {
                                    "type": "ws_message",
                                    "id": rid,
                                    "data": data,
                                    "binary": binary,
                                }
                            )

                    async def from_sandbox() -> None:
                        while True:
                            channel_frame = await queue.get()
                            frame_type = channel_frame.get("type")
                            if frame_type == "ws_message":
                                data = channel_frame.get("data", "")
                                if channel_frame.get("binary", False):
                                    data = base64.b64decode(data, validate=True)
                                await upstream_ws.send(data)
                            elif frame_type == "ws_close":
                                code = channel_frame.get("code", 1000)
                                reason = channel_frame.get("reason", "")
                                await upstream_ws.close(code=code, reason=reason)
                                return

                    upstream_task = asyncio.create_task(from_upstream())
                    sandbox_task = asyncio.create_task(from_sandbox())
                    done, pending = await asyncio.wait(
                        (upstream_task, sandbox_task),
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for task in pending:
                        task.cancel()
                    if pending:
                        await asyncio.gather(*pending, return_exceptions=True)
                    for task in done:
                        task.result()

                    await send_frame(
                        {
                            "type": "ws_close",
                            "id": rid,
                            "code": upstream_ws.close_code or 1000,
                            "reason": upstream_ws.close_reason or "",
                        }
                    )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("tunnel ws_connect upstream error: %s", exc)
                try:
                    await send_frame(
                        {
                            "type": "error",
                            "id": rid,
                            "message": str(exc),
                        }
                    )
                except ws_exc.ConnectionClosed:
                    pass
            finally:
                ws_channels.pop(rid, None)

        limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
        tasks: set = set()
        try:
            async with httpx.AsyncClient(
                limits=limits,
                timeout=httpx.Timeout(60.0),
                trust_env=False,
            ) as client:
                async for message in ws:
                    if self._stopping.is_set():
                        break
                    text = (
                        message.decode("utf-8", "replace")
                        if isinstance(message, bytes)
                        else message
                    )
                    try:
                        frame = json.loads(text)
                    except (ValueError, TypeError):
                        continue
                    ftype = frame.get("type")
                    if ftype == "http_req":
                        t = handle_http_req(client, frame)
                        tasks.add(t)
                        t.add_done_callback(tasks.discard)
                    elif ftype == "ws_connect":
                        t = asyncio.create_task(handle_ws_connect(frame))
                        tasks.add(t)
                        t.add_done_callback(tasks.discard)
                    elif ftype in ("ws_message", "ws_close"):
                        channel = ws_channels.get(frame.get("id", ""))
                        if channel is not None:
                            await channel.put(frame)
                    elif ftype == "ping":
                        await send_frame(
                            {
                                "type": "pong",
                                "id": frame.get("id", ""),
                                "timestamp": frame.get("timestamp", 0),
                            }
                        )
        except ws_exc.ConnectionClosed:
            pass
        finally:
            for task in list(tasks):
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            ws_channels.clear()
