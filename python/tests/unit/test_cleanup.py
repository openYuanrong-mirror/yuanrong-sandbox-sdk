"""Explicit sandbox cleanup and GC reentrancy regressions."""

import gc
import os
import subprocess
import sys
import threading
import unittest
import weakref
from unittest.mock import Mock, patch

from yr_sandbox import ConnectionConfig, Sandbox, SandboxError
from yr_sandbox._http_pool import _SHARED_HTTP_CLIENT_REGISTRY
from yr_sandbox._transport import SandboxClient


def _make_sandbox(*, detached=False):
    sandbox = object.__new__(Sandbox)
    sandbox._closed = False
    sandbox._detached = detached
    sandbox._sid = "cleanup-test"
    sandbox._tunnel_client = Mock()
    sandbox._shells = Mock()
    sandbox._pty = Mock()
    sandbox._client = Mock()
    return sandbox


def _gc_pool_lock_probe():
    # Use the real constructor, SandboxClient, shared lease and HTTPX pool;
    # only creation is stubbed so this regression needs no running cluster.
    gc.disable()
    connection = ConnectionConfig(
        server_address="127.0.0.1:1", token="test", use_tls=False,
    )
    with patch.dict(os.environ, {"NO_PROXY": "*", "no_proxy": "*"}):
        for cyclic in (False, True):
            with patch.object(
                SandboxClient, "create_info",
                return_value={"sandboxId": "gc-test", "status": "running"},
            ):
                sandbox = Sandbox(connection=connection)
            if cyclic:
                sandbox.cycle = sandbox
            reference = weakref.ref(sandbox)
            http = sandbox._client._http._current_client()
            with http._transport._pool._optional_thread_lock:
                del sandbox
                gc.collect()
            if reference() is not None:
                raise AssertionError("sandbox was not collected")
    _SHARED_HTTP_CLIENT_REGISTRY.close_all()


class CleanupTests(unittest.TestCase):
    def test_gc_under_real_http_pool_lock_completes(self):
        # The old destructor deadlocks. Bound it in a child process so a
        # regression fails the test instead of hanging the entire test suite.
        result = subprocess.run(
            [sys.executable, __file__, "--gc-pool-lock-probe"],
            capture_output=True, text=True, timeout=10, check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_gc_does_not_delete_or_close_resources(self):
        sandbox = _make_sandbox()
        client, tunnel, shells, pty = (
            sandbox._client, sandbox._tunnel_client,
            sandbox._shells, sandbox._pty,
        )
        reference = weakref.ref(sandbox)
        sandbox.cycle = sandbox
        del sandbox
        gc.collect()
        self.assertIsNone(reference())
        client.delete.assert_not_called()
        client.close.assert_not_called()
        tunnel.stop.assert_not_called()
        shells.close.assert_not_called()
        pty._close.assert_not_called()

    def test_context_exit_deletes_synchronously_and_closes(self):
        sandbox = _make_sandbox()
        threads = []
        sandbox._client.delete.side_effect = lambda _sid: threads.append(
            threading.get_ident()
        )
        with sandbox:
            sandbox._client.delete.assert_not_called()
        self.assertEqual(threads, [threading.get_ident()])
        sandbox._client.close.assert_called_once_with()

    def test_context_exit_propagates_cleanup_error_without_body_error(self):
        sandbox = _make_sandbox()
        error = SandboxError("delete failed")
        sandbox._client.delete.side_effect = error
        with self.assertRaises(SandboxError) as raised, sandbox:
            pass
        self.assertIs(raised.exception, error)
        sandbox._client.close.assert_called_once_with()

    def test_context_exit_preserves_body_error_and_logs_cleanup_error(self):
        sandbox = _make_sandbox()
        error = ValueError("workload failed")
        sandbox._client.delete.side_effect = SandboxError("delete failed")
        with (
            self.assertLogs("yr_sandbox.sandbox_api", level="WARNING") as logs,
            self.assertRaises(ValueError) as raised,
            sandbox,
        ):
            raise error
        self.assertIs(raised.exception, error)
        self.assertIn("cleanup-test", logs.output[0])
        sandbox._client.close.assert_called_once_with()

    def test_local_resource_errors_do_not_skip_delete(self):
        sandbox = _make_sandbox()
        sandbox._tunnel_client.stop.side_effect = RuntimeError("tunnel failed")
        sandbox._shells.close.side_effect = RuntimeError("shell failed")
        sandbox._pty._close.side_effect = RuntimeError("pty failed")
        sandbox.kill()
        sandbox._client.delete.assert_called_once_with("cleanup-test")
        sandbox._client.close.assert_called_once_with()

    def test_kill_preserves_delete_error_when_client_close_also_fails(self):
        sandbox = _make_sandbox()
        error = SandboxError("delete failed")
        sandbox._client.delete.side_effect = error
        sandbox._client.close.side_effect = RuntimeError("close failed")
        with (
            self.assertLogs("yr_sandbox.sandbox_api", level="WARNING"),
            self.assertRaises(SandboxError) as raised,
        ):
            sandbox.kill()
        self.assertIs(raised.exception, error)
        sandbox._client.close.assert_called_once_with()

    def test_delete_preserves_delete_error_when_client_close_also_fails(self):
        client = Mock()
        error = SandboxError("delete failed")
        client.delete.side_effect = error
        client.close.side_effect = RuntimeError("close failed")
        with (
            patch("yr_sandbox.sandbox_api.SandboxClient", return_value=client),
            self.assertLogs("yr_sandbox.sandbox_api", level="WARNING"),
            self.assertRaises(SandboxError) as raised,
        ):
            Sandbox.delete("cleanup-test")
        self.assertIs(raised.exception, error)
        client.close.assert_called_once_with()

    def test_client_close_failure_is_visible_after_successful_delete(self):
        sandbox = _make_sandbox()
        error = RuntimeError("close failed")
        sandbox._client.close.side_effect = error
        with self.assertRaises(RuntimeError) as raised:
            sandbox.kill()
        self.assertIs(raised.exception, error)
        sandbox._client.delete.assert_called_once_with("cleanup-test")


if __name__ == "__main__":
    if sys.argv[1:] == ["--gc-pool-lock-probe"]:
        _gc_pool_lock_probe()
    else:
        unittest.main()
