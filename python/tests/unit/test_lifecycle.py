import inspect
import os
import unittest
from unittest.mock import patch

from yr_sandbox import ConnectionConfig, Sandbox


class _CloseTracker:
    def __init__(self):
        self.closed = False
        self.close_count = 0
        self.deleted = []

    def close(self):
        self.closed = True
        self.close_count += 1

    def delete(self, sandbox_id):
        self.deleted.append(sandbox_id)


class _Shells:
    def close(self):
        pass


class _PTY:
    def _close(self):
        pass


class LifecycleTests(unittest.TestCase):
    def test_delete_uses_sandbox_id_without_name_namespace_variant(self):
        self.assertEqual(
            list(inspect.signature(Sandbox.delete).parameters),
            ["sandbox_id", "connection"],
        )

    def test_delete_accepts_explicit_connection_config(self):
        seen = {}

        class Client(_CloseTracker):
            def __init__(self, *, connection):
                super().__init__()
                seen["connection"] = connection
                seen["client"] = self

        connection = ConnectionConfig(
            server_address="frontend.example:443",
            token="secret",
        )
        with (
            patch("yr_sandbox.sandbox_api.SandboxClient", Client),
            patch.dict(os.environ, {}, clear=True),
        ):
            Sandbox.delete("sandbox-1", connection=connection)

        self.assertIs(seen["connection"], connection)
        self.assertEqual(seen["client"].deleted, ["sandbox-1"])
        self.assertTrue(seen["client"].closed)

    def test_detached_kill_closes_local_client_without_deleting_remote(self):
        sandbox = object.__new__(Sandbox)
        sandbox._detached = True
        sandbox._tunnel_client = None
        sandbox._shells = _Shells()
        sandbox._pty = _PTY()
        sandbox._client = _CloseTracker()
        sandbox._sid = "sandbox-1"
        sandbox._closed = False

        sandbox.kill()

        self.assertTrue(sandbox._client.closed)

    def test_close_releases_local_resources_without_deleting_remote(self):
        sandbox = object.__new__(Sandbox)
        sandbox._detached = False
        sandbox._tunnel_client = None
        sandbox._shells = _Shells()
        sandbox._pty = _PTY()
        sandbox._client = _CloseTracker()
        sandbox._sid = "sandbox-1"
        sandbox._closed = False

        sandbox.close()
        sandbox.close()

        self.assertEqual(sandbox._client.deleted, [])
        self.assertEqual(sandbox._client.close_count, 1)

    def test_kill_is_idempotent(self):
        sandbox = object.__new__(Sandbox)
        sandbox._detached = False
        sandbox._tunnel_client = None
        sandbox._shells = _Shells()
        sandbox._pty = _PTY()
        sandbox._client = _CloseTracker()
        sandbox._sid = "sandbox-1"
        sandbox._closed = False

        sandbox.kill()
        sandbox.kill()

        self.assertEqual(sandbox._client.deleted, ["sandbox-1"])
        self.assertEqual(sandbox._client.close_count, 1)

    def test_tunnel_start_failure_rolls_back_created_sandbox(self):
        tracker = _CloseTracker()

        class Client:
            token = "token"

            def __init__(self):
                pass

            def create_info(self, _body):
                return {"sandboxId": "sandbox-1", "status": "running"}

            def delete(self, sandbox_id):
                tracker.delete(sandbox_id)

            def close(self):
                tracker.close()

            @staticmethod
            def _safe_id(sandbox_id):
                return sandbox_id

        class Tunnel:
            def __init__(self, _target, token=None):
                self.token = token

            def start(self, _url, timeout=60):
                return False

            def stop(self):
                pass

        with (
            patch("yr_sandbox.sandbox_api.SandboxClient", Client),
            patch("yr_sandbox.tunnel_client.TunnelClient", Tunnel),
            patch.dict(
                os.environ,
                {"YR_GATEWAY_ADDRESS": "frontend:8080", "YR_GATEWAY_TLS": "0"},
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "connection timeout"):
                Sandbox(
                    image="ubuntu:22.04",
                    upstream="127.0.0.1:9000",
                    tunnel_connect_timeout=0.1,
                    detached=True,
                )

        self.assertEqual(tracker.deleted, ["sandbox-1"])
        self.assertEqual(tracker.close_count, 1)

    def test_tunnel_uses_explicit_gateway_and_token_without_environment(self):
        seen = {}

        class Client(_CloseTracker):
            def __init__(self, *, connection):
                super().__init__()
                self.token = connection.token

            def create_info(self, _body):
                return {
                    "sandboxId": "sandbox-1",
                    "status": "running",
                    "tunnel": {"path": "/tunnel/sandbox-1"},
                }

            def set_direct_enabled(self, _enabled):
                pass

            @staticmethod
            def _safe_id(sandbox_id):
                return sandbox_id

        class Tunnel:
            def __init__(self, _target, token=None):
                seen["token"] = token

            def start(self, url, timeout=60):
                seen["url"] = url
                return True

            def stop(self):
                pass

        connection = ConnectionConfig(
            server_address="frontend.example:443",
            token="secret",
            gateway_address="gateway.example:8443",
            gateway_use_tls=True,
        )
        with (
            patch("yr_sandbox.sandbox_api.SandboxClient", Client),
            patch("yr_sandbox.tunnel_client.TunnelClient", Tunnel),
            patch.dict(os.environ, {}, clear=True),
        ):
            sandbox = Sandbox(
                image="ubuntu:22.04",
                upstream="127.0.0.1:9000",
                connection=connection,
                detached=True,
            )
            sandbox.close()

        self.assertEqual(seen["url"], "wss://gateway.example:8443/tunnel/sandbox-1")
        self.assertEqual(seen["token"], "secret")


if __name__ == "__main__":
    unittest.main()
