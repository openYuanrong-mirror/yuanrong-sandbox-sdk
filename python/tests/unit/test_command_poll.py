"""Command waits stop on client closure and pace failed requests."""

import threading
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest

from yr_sandbox import _http_pool, commands
from yr_sandbox._http_pool import SandboxClientClosedError
from yr_sandbox._transport import SandboxClient, SandboxError
from yr_sandbox.commands import CommandHandle, Commands
from yr_sandbox.types import CommandResult, CommandStatus


@pytest.fixture
def clock(monkeypatch):
    state = SimpleNamespace(now=0.0, sleeps=[])

    def sleep(delay):
        state.sleeps.append(delay)
        state.now += delay

    monkeypatch.setattr(
        commands, "time", SimpleNamespace(monotonic=lambda: state.now, sleep=sleep)
    )
    return state


@pytest.fixture
def running(monkeypatch):
    handle = Mock(spec=CommandHandle)
    handle.id = "cmd-42"
    collection = Commands(Mock(spec=SandboxClient), "sandbox")
    monkeypatch.setattr(collection, "run", Mock(return_value=handle))
    return collection, handle


def test_long_command_client_closed_exits_without_retry_or_kill(clock, running, caplog):
    collection, handle = running
    error = SandboxClientClosedError("client closed")
    handle.wait.side_effect = error
    with pytest.raises(SandboxClientClosedError) as raised:
        collection._run_with_poll("sleep 60", None, None, 60)
    assert raised.value is error
    assert handle.wait.call_count == 1
    handle.kill.assert_not_called()
    assert clock.sleeps == []
    assert "command wait failed" not in caplog.text


@pytest.mark.parametrize("error", [
    httpx.ReadTimeout("timeout"), httpx.ConnectError("reset"),
    SandboxError("gateway unavailable"), RuntimeError("request failed"),
    ValueError("bad data"),
])
def test_wait_error_retries_then_returns_result(clock, running, error):
    collection, handle = running
    expected = CommandResult("done", "", 0)
    handle.wait.side_effect = [error, expected]
    assert collection._run_with_poll("sleep 60", None, None, 60) is expected
    assert handle.wait.call_count == 2
    assert clock.sleeps == [1]
    handle.kill.assert_not_called()


def test_persistent_failure_is_paced_within_deadline(clock, running):
    collection, handle = running

    def wait(_timeout):
        clock.now += 0.1
        raise RuntimeError("request failed")

    handle.wait.side_effect = wait
    result = collection._run_with_poll("sleep 60", None, None, 3)
    assert result.status == CommandStatus.TIMED_OUT
    assert clock.now == pytest.approx(3)
    assert clock.sleeps == pytest.approx([1, 1, 0.7])
    assert handle.wait.call_count == 3
    handle.kill.assert_called_once_with()


def test_failure_after_deadline_does_not_delay(clock, running):
    collection, handle = running

    def wait(_timeout):
        clock.now += 3
        raise RuntimeError("request failed")

    handle.wait.side_effect = wait
    result = collection._run_with_poll("sleep 60", None, None, 3)
    assert result.status == CommandStatus.TIMED_OUT
    assert clock.sleeps == []
    assert handle.wait.call_count == 1
    handle.kill.assert_called_once_with()


def test_wait_timeout_continues_waiting(clock, running):
    collection, handle = running
    expected = CommandResult("done", "", 0)
    handle.wait.side_effect = [TimeoutError("still running"), expected]
    assert collection._run_with_poll("sleep 60", None, None, 60) is expected
    assert clock.sleeps == []
    handle.kill.assert_not_called()


@pytest.mark.parametrize("direct", [False, True])
def test_close_during_poll_stops_wait_thread_and_preserves_other_lease(
    monkeypatch, direct
):
    entered = threading.Event()
    release = threading.Event()
    errors = []

    def handle(request):
        entered.set()
        assert release.wait(timeout=5)
        payload = {"status": "running"}
        if request.url.path.startswith("/api/"):
            payload = {"code": 200, "data": payload}
        return httpx.Response(200, json=payload)

    registry = _http_pool._SharedHTTPClientRegistry()
    monkeypatch.setattr(_http_pool, "_SHARED_HTTP_CLIENT_REGISTRY", registry)
    monkeypatch.setattr(
        _http_pool, "_new_http_client",
        lambda _verify: httpx.Client(transport=httpx.MockTransport(handle)),
    )
    client = SandboxClient(server="poll.example", token="first")
    other = SandboxClient(server="poll.example", token="second")
    client._direct_enabled = direct
    client._connection = None
    invoke = Mock(wraps=client.invoke)
    monkeypatch.setattr(client, "invoke", invoke)

    def wait():
        try:
            CommandHandle("cmd-42", client, "sandbox", 42).wait(timeout=3)
        except Exception as exc:
            errors.append(exc)

    worker = threading.Thread(target=wait, daemon=True)
    worker.start()
    try:
        assert entered.wait(timeout=5)
        client.close()
        release.set()
        worker.join(timeout=5)
        assert not worker.is_alive()
        assert len(errors) == 1
        assert isinstance(errors[0], SandboxClientClosedError)
        assert isinstance(errors[0], RuntimeError)
        assert "SandboxClient.close()" in str(errors[0])
        assert [call.args[1] for call in invoke.call_args_list] == ["process.get", "process.wait"]
        assert other.invoke("other", "process.poll", {"pid": 43}) == {"status": "running"}
    finally:
        release.set()
        worker.join(timeout=5)
        client.close()
        other.close()
        registry.close_all()
