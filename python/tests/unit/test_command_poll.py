"""Command polling stops on client closure and paces failed requests."""

import threading
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest

from yr_sandbox import _http_pool, commands
from yr_sandbox._http_pool import SandboxClientClosedError
from yr_sandbox._transport import SandboxClient, SandboxError
from yr_sandbox.commands import CommandHandle, Commands


DONE = {"status": "done", "stdout": "output", "stderr": "", "exit_code": 0}


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


@pytest.mark.parametrize("entry_point", ["wait", "run"])
def test_client_closed_escapes_without_retry_or_kill(clock, entry_point, caplog):
    error = SandboxClientClosedError("Cannot send a request after SandboxClient.close()")
    responses = [error, DONE]
    if entry_point == "run":
        responses.insert(0, {"pid": 42})
    client = Mock(spec=SandboxClient)
    client.invoke.side_effect = responses

    with pytest.raises(SandboxClientClosedError) as raised:
        if entry_point == "wait":
            CommandHandle(42, client, "sandbox").wait(timeout=60)
        else:
            Commands(client, "sandbox").run("sleep 60", timeout=60)

    assert raised.value is error
    actions = [call.args[1] for call in client.invoke.call_args_list]
    expected = (["process.start"] if entry_point == "run" else []) + ["process.poll"]
    assert actions == expected
    assert clock.sleeps == []
    assert "process.poll failed" not in caplog.text


@pytest.mark.parametrize(
    "error",
    [
        httpx.ReadTimeout("timeout"),
        httpx.ConnectError("reset"),
        SandboxError("gateway unavailable"),
        RuntimeError("request failed"),
        ValueError("bad data"),
    ],
)
def test_poll_error_retries_then_returns_output(clock, error):
    client = Mock(spec=SandboxClient)
    client.invoke.side_effect = [error, {"status": "running"}, DONE]

    result = CommandHandle(42, client, "sandbox").wait(timeout=60)

    assert (result.stdout, result.stderr, result.exit_code) == ("output", "", 0)
    assert clock.sleeps == [1]
    assert [call.args[1] for call in client.invoke.call_args_list] == ["process.poll"] * 3


def test_persistent_failure_is_paced_and_delay_respects_deadline(clock):
    client = Mock(spec=SandboxClient)

    def invoke(_sid, action, _args, **_kwargs):
        if action == "process.kill":
            return {"killed": True}
        clock.now += 0.1
        raise httpx.ReadError("connection reset")

    client.invoke.side_effect = invoke
    result = CommandHandle(42, client, "sandbox").wait(timeout=3)

    assert result.exit_code == -1
    assert result.stderr == "Command timed out after 3 seconds"
    assert clock.now == pytest.approx(3)
    assert clock.sleeps == pytest.approx([1, 1, 0.7])
    assert [call.args[1] for call in client.invoke.call_args_list] == [
        "process.poll", "process.poll", "process.poll", "process.kill"
    ]


def test_poll_failure_after_deadline_does_not_sleep_or_retry(clock):
    client = Mock(spec=SandboxClient)

    def invoke(_sid, action, _args, **_kwargs):
        if action == "process.kill":
            raise SandboxClientClosedError("client closed during timeout cleanup")
        clock.now += 3
        raise httpx.ReadTimeout("timeout")

    client.invoke.side_effect = invoke
    result = CommandHandle(42, client, "sandbox").wait(timeout=3)

    assert result.exit_code == -1
    assert clock.sleeps == []
    assert [call.args[1] for call in client.invoke.call_args_list] == ["process.poll", "process.kill"]


def test_remote_process_error_returns_failure_without_retry(clock):
    client = Mock(spec=SandboxClient)
    client.invoke.return_value = {"status": "error", "error": "No process with pid 42"}

    result = CommandHandle(42, client, "sandbox").wait(timeout=60)

    assert result.exit_code == -1
    assert result.stderr == "No process with pid 42"
    assert client.invoke.call_count == 1
    assert clock.sleeps == []


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
    invoke = Mock(wraps=client.invoke)
    monkeypatch.setattr(client, "invoke", invoke)

    def wait():
        try:
            CommandHandle(42, client, "sandbox").wait(timeout=3)
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
        assert [call.args[1] for call in invoke.call_args_list] == ["process.poll"] * 2
        assert other.invoke("other", "process.poll", {"pid": 43}) == {"status": "running"}
    finally:
        release.set()
        worker.join(timeout=5)
        client.close()
        other.close()
        registry.close_all()
