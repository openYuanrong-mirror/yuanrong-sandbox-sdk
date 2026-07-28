"""Focused reverse-tunnel client protocol tests."""

import asyncio
import base64
import gzip
import http.server
import json
import os
import ssl
import threading
import unittest
from typing import ClassVar
from unittest import mock

from yr_sandbox import tunnel_client
from yr_sandbox.tunnel_client import TunnelClient


class _RecordingHandler(http.server.BaseHTTPRequestHandler):
    requests: ClassVar[list] = []

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_length)
        type(self).requests.append((self.path, self.headers, body))
        if self.path == "/gzip":
            response = gzip.compress(b"compressed-response")
            self.send_response(200)
            self.send_header("Content-Encoding", "gzip")
            self.send_header("Content-Length", str(len(response)))
            self.send_header("Set-Cookie", "session=one; Path=/")
            self.send_header("Set-Cookie", "theme=dark; Path=/")
            self.end_headers()
            self.wfile.write(response)
            return
        response = b"ok"
        self.send_response(200)
        if self.path == "/set-cookie":
            self.send_header("Set-Cookie", "session=leaked; Path=/")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def log_message(self, *_args):
        return


class _FrameWebSocket:
    def __init__(self):
        self._incoming = asyncio.Queue()
        self.sent = asyncio.Queue()

    def __aiter__(self):
        return self

    async def __anext__(self):
        message = await self._incoming.get()
        if message is None:
            raise StopAsyncIteration
        return json.dumps(message)

    async def send(self, message):
        await self.sent.put(json.loads(message))

    def feed(self, frame):
        self._incoming.put_nowait(frame)

    def close_input(self):
        self._incoming.put_nowait(None)


class TunnelClientRequestTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        _RecordingHandler.requests = []
        self.server = http.server.ThreadingHTTPServer(
            ("127.0.0.1", 0),
            _RecordingHandler,
        )
        self.server_thread = threading.Thread(
            target=self.server.serve_forever,
            daemon=True,
        )
        self.server_thread.start()

    async def asyncTearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.server_thread.join(timeout=2)

    async def test_pair_headers_preserve_duplicates_and_strip_hop_by_hop(self):
        payload = b"request-body"
        frame = {
            "type": "http_req",
            "id": "request-1",
            "method": "POST",
            "path": "/headers",
            "headers": [
                ["Connection", "close, X-First-Hop"],
                ["Connection", "X-Second-Hop"],
                ["X-First-Hop", "drop-one"],
                ["X-Second-Hop", "drop-two"],
                ["Transfer-Encoding", "chunked"],
                ["Content-Length", "999"],
                ["X-Tag", "first"],
                ["X-Tag", "second"],
                ["Authorization", "Bearer test"],
                ["Content-Type", "application/octet-stream"],
            ],
            "body": base64.b64encode(payload).decode("ascii"),
        }
        websocket = _FrameWebSocket()
        websocket.feed(frame)
        client = TunnelClient(
            upstream=f"127.0.0.1:{self.server.server_port}",
        )

        proxy_task = asyncio.create_task(client._proxy_loop(websocket))
        try:
            response = await asyncio.wait_for(websocket.sent.get(), timeout=2)
        finally:
            websocket.close_input()
            await asyncio.wait_for(proxy_task, timeout=2)

        self.assertEqual(response["type"], "http_resp")
        self.assertEqual(len(_RecordingHandler.requests), 1)
        _, headers, body = _RecordingHandler.requests[0]
        self.assertEqual(body, payload)
        self.assertEqual(headers.get_all("X-Tag"), ["first", "second"])
        self.assertIsNone(headers.get("X-First-Hop"))
        self.assertIsNone(headers.get("X-Second-Hop"))
        self.assertIsNone(headers.get("Transfer-Encoding"))
        self.assertEqual(headers.get("Content-Length"), str(len(payload)))
        self.assertEqual(headers.get("Authorization"), "Bearer test")
        self.assertEqual(
            headers.get("Content-Type"),
            "application/octet-stream",
        )

    async def test_response_keeps_raw_gzip_and_duplicate_set_cookie(self):
        websocket = _FrameWebSocket()
        websocket.feed(
            {
                "type": "http_req",
                "id": "gzip-1",
                "method": "POST",
                "path": "/gzip",
                "headers": [],
                "body": "",
            }
        )
        client = TunnelClient(
            upstream=f"127.0.0.1:{self.server.server_port}",
        )

        proxy_task = asyncio.create_task(client._proxy_loop(websocket))
        try:
            response = await asyncio.wait_for(websocket.sent.get(), timeout=2)
        finally:
            websocket.close_input()
            await asyncio.wait_for(proxy_task, timeout=2)

        raw_body = base64.b64decode(response["body"])
        self.assertEqual(gzip.decompress(raw_body), b"compressed-response")
        set_cookies = [
            value for name, value in response["headers"] if name.lower() == "set-cookie"
        ]
        self.assertEqual(
            set_cookies,
            ["session=one; Path=/", "theme=dark; Path=/"],
        )

    async def test_shared_pool_does_not_replay_response_cookies(self):
        websocket = _FrameWebSocket()
        client = TunnelClient(
            upstream=f"127.0.0.1:{self.server.server_port}",
        )
        proxy_task = asyncio.create_task(client._proxy_loop(websocket))
        try:
            websocket.feed(
                {
                    "type": "http_req",
                    "id": "cookie-1",
                    "method": "POST",
                    "path": "/set-cookie",
                    "headers": [],
                    "body": "",
                }
            )
            await asyncio.wait_for(websocket.sent.get(), timeout=2)
            websocket.feed(
                {
                    "type": "http_req",
                    "id": "cookie-2",
                    "method": "POST",
                    "path": "/check-cookie",
                    "headers": [],
                    "body": "",
                }
            )
            await asyncio.wait_for(websocket.sent.get(), timeout=2)
        finally:
            websocket.close_input()
            await asyncio.wait_for(proxy_task, timeout=2)

        self.assertEqual(len(_RecordingHandler.requests), 2)
        _, second_headers, _ = _RecordingHandler.requests[1]
        self.assertIsNone(second_headers.get("Cookie"))

    async def test_explicit_cookie_header_is_forwarded(self):
        websocket = _FrameWebSocket()
        websocket.feed(
            {
                "type": "http_req",
                "id": "cookie-explicit",
                "method": "POST",
                "path": "/check-cookie",
                "headers": [["Cookie", "caller=explicit"]],
                "body": "",
            }
        )
        client = TunnelClient(
            upstream=f"127.0.0.1:{self.server.server_port}",
        )

        proxy_task = asyncio.create_task(client._proxy_loop(websocket))
        try:
            await asyncio.wait_for(websocket.sent.get(), timeout=2)
        finally:
            websocket.close_input()
            await asyncio.wait_for(proxy_task, timeout=2)

        _, headers, _ = _RecordingHandler.requests[0]
        self.assertEqual(headers.get("Cookie"), "caller=explicit")


class TunnelClientTlsTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _rejected_connection(client, status_code):
        from websockets.datastructures import Headers
        from websockets.http11 import Response

        error = tunnel_client.ws_exc.InvalidStatus(
            Response(status_code, "Rejected", Headers(), b"")
        )

        class _RejectedConnection:
            async def __aenter__(self):
                client._stopping.set()
                raise error

            async def __aexit__(self, *_args):
                return False

        return _RejectedConnection()

    async def test_route_not_ready_404_is_debug_only(self):
        client = TunnelClient(upstream="127.0.0.1:1")

        with (
            mock.patch.object(
                tunnel_client.ws_client,
                "connect",
                return_value=self._rejected_connection(client, 404),
            ),
            self.assertLogs(tunnel_client.logger, level="DEBUG") as logs,
        ):
            await client._connect_loop("ws://router.test/tunnel/sandbox")

        output = "\n".join(logs.output)
        self.assertIn("route unavailable", output)
        self.assertNotIn("unexpected error", output)

    async def test_non_404_handshake_rejection_stays_visible(self):
        client = TunnelClient(upstream="127.0.0.1:1")

        with (
            mock.patch.object(
                tunnel_client.ws_client,
                "connect",
                return_value=self._rejected_connection(client, 403),
            ),
            self.assertLogs(tunnel_client.logger, level="WARNING") as logs,
        ):
            await client._connect_loop("ws://router.test/tunnel/sandbox")

        output = "\n".join(logs.output)
        self.assertIn("handshake rejected", output)
        self.assertIn("HTTP 403", output)

    async def test_wss_uses_default_certificate_and_hostname_verification(self):
        client = TunnelClient(upstream="127.0.0.1:1")
        captured = {}

        class _FailingConnection:
            async def __aenter__(self):
                client._stopping.set()
                raise OSError("stop after inspecting connection arguments")

            async def __aexit__(self, *_args):
                return False

        def fake_connect(_url, **kwargs):
            captured.update(kwargs)
            return _FailingConnection()

        with mock.patch.object(
            tunnel_client.ws_client,
            "connect",
            side_effect=fake_connect,
        ):
            await client._connect_loop("wss://tunnel.example.test/path")

        context = captured["ssl"]
        self.assertIsInstance(context, ssl.SSLContext)
        self.assertTrue(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)

    def test_wss_uses_explicit_ca_bundle(self):
        expected = ssl.create_default_context()
        with (
            mock.patch.dict(
                os.environ,
                {"YR_TUNNEL_CA_BUNDLE": "/tmp/tunnel-ca.pem"},
                clear=False,
            ),
            mock.patch.object(
                tunnel_client.ssl,
                "create_default_context",
                return_value=expected,
            ) as create_context,
        ):
            context = tunnel_client._ssl_context_for_tunnel(
                "wss://tunnel.example.test/path"
            )

        self.assertIs(context, expected)
        create_context.assert_called_once_with(cafile="/tmp/tunnel-ca.pem")

    def test_wss_insecure_mode_requires_explicit_development_switch(self):
        with (
            mock.patch.dict(
                os.environ,
                {"YR_TUNNEL_SSL_VERIFY": "0"},
                clear=False,
            ),
            self.assertLogs(tunnel_client.logger, level="WARNING") as logs,
        ):
            context = tunnel_client._ssl_context_for_tunnel(
                "wss://tunnel.example.test/path"
            )

        self.assertFalse(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_NONE)
        self.assertIn("explicitly disabled", "\n".join(logs.output))


if __name__ == "__main__":
    unittest.main()
