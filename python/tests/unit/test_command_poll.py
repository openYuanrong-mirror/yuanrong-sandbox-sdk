"""Command polling stops on client closure and paces failed requests."""

import threading
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest

from yr_sandbox import _http_pool, commands
from yr_sandbox._http_pool import SandboxClientClosedError
from yr_sandbox._transport import SandboxClient, SandboxError, SandboxHTTPError
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
    result = CommandHandle(42, client, "sandbox").wait(timeout=2)

    assert result.exit_code == -1
    assert result.stderr == "Command timed out after 2 seconds"
    assert clock.now == pytest.approx(2)
    assert clock.sleeps == pytest.approx([1, 0.8])
    assert [call.args[1] for call in client.invoke.call_args_list] == [
        "process.poll", "process.poll", "process.kill"
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


@pytest.mark.parametrize("entry_point", ["wait", "run"])
@pytest.mark.parametrize("direct", [False, True])
@pytest.mark.parametrize("http_status,code,state", [
    (410, "SANDBOX_EXITED", "FATAL"),
    (409, "SANDBOX_SCHEDULE_FAILED", "SCHEDULE_FAILED"),
    (503, "SANDBOX_EXITED", "FATAL"),
])
def test_terminal_response_stops_polling(monkeypatch, clock, caplog, entry_point,
                                        direct, http_status, code, state):
    requests = []
    body = {"code": code, "state": state, "retryable": False,
            "message": "sandbox workload exited", "instance_id": "sandbox",
            "exit_code": 137, "exit_type": 1, "err_code": 1501}

    def handle(request):
        requests.append(request)
        if entry_point == "run" and len(requests) == 1:
            payload = {"pid": 42}
            return httpx.Response(200, json=payload if direct else {"code": 200, "data": payload})
        return httpx.Response(http_status, json=body)

    registry = _http_pool._SharedHTTPClientRegistry()
    monkeypatch.setattr(_http_pool, "_SHARED_HTTP_CLIENT_REGISTRY", registry)
    monkeypatch.setattr(_http_pool, "_new_http_client",
                        lambda _verify: httpx.Client(transport=httpx.MockTransport(handle)))
    client = SandboxClient(server="poll.example", token="test")
    client._direct_enabled = direct
    try:
        with pytest.raises(SandboxHTTPError) as raised:
            if entry_point == "wait":
                CommandHandle(42, client, "sandbox").wait(timeout=7200)
            else:
                Commands(client, "sandbox").run("sleep 60", timeout=7200)
        assert raised.value.terminal
        assert raised.value.body == body
        assert raised.value.status_code == http_status
        assert len(requests) == (2 if entry_point == "run" else 1)
        assert all(request.method == "POST" for request in requests)
        assert clock.sleeps == []
        assert "process.poll failed" not in caplog.text
    finally:
        client.close()
        registry.close_all()


def test_recovering_response_can_retry_then_succeed(clock):
    client = Mock(spec=SandboxClient)
    body = {"code": "SANDBOX_RECOVERING", "state": "FAILED", "retryable": True}
    with pytest.raises(SandboxHTTPError) as raised:
        SandboxClient._json(httpx.Response(503, json=body))
    error = raised.value
    assert not error.terminal
    client.invoke.side_effect = [error, DONE]

    assert CommandHandle(42, client, "sandbox").wait(timeout=60).stdout == "output"
    assert clock.sleeps == [1]
    client.instance_info.assert_not_called()


def test_consecutive_failures_are_bounded(clock, caplog):
    client = Mock(spec=SandboxClient)
    error = httpx.ReadError("connection reset")
    client.invoke.side_effect = error

    with pytest.raises(SandboxError, match="after 3 consecutive errors") as raised:
        CommandHandle(42, client, "sandbox").wait(timeout=7200)

    assert raised.value.__cause__ is error
    assert [call.args[1] for call in client.invoke.call_args_list] == ["process.poll"] * 3
    client.instance_info.assert_not_called()
    assert clock.sleeps == [1, 1]
    assert caplog.text.count("process.poll failed") == 2


def test_successful_poll_resets_consecutive_error_budget(clock):
    client = Mock(spec=SandboxClient)
    error = httpx.ReadError("reset")
    client.invoke.side_effect = [error, error, {"status": "running"}, error, error, DONE]

    assert CommandHandle(42, client, "sandbox").wait(timeout=60).stdout == "output"
    assert clock.sleeps == [1] * 4


@pytest.mark.parametrize("error", [
    ValueError("bad data"), RuntimeError("unexpected failure"),
    SandboxHTTPError("HTTP 401", 401), SandboxHTTPError("HTTP 403", 403),
    SandboxHTTPError("HTTP 400", 400),
])
def test_nonretryable_error_escapes_without_retry(clock, error):
    client = Mock(spec=SandboxClient)
    client.invoke.side_effect = error

    with pytest.raises(type(error)) as raised:
        CommandHandle(42, client, "sandbox").wait(timeout=60)

    assert raised.value is error
    assert client.invoke.call_count == 1
    client.instance_info.assert_not_called()
    assert clock.sleeps == []


@pytest.mark.parametrize("http_status,code", [(403, 403), (200, 403)])
def test_frontend_http_error_retains_status_code(http_status, code):
    response = httpx.Response(http_status, json={"code": code, "message": "forbidden"})
    with pytest.raises(SandboxHTTPError) as raised:
        SandboxClient._json(response)
    assert raised.value.status_code == 403


@pytest.mark.parametrize("body", [
    {"message": "fatal error in gateway"},
    {"code": "SANDBOX_RECOVERING", "retryable": True},
    {"code": "SANDBOX_EXITED", "retryable": "false"},
    {"code": "SANDBOX_EXITED"},
])
def test_generic_response_does_not_establish_terminal_state(body):
    error = SandboxHTTPError.from_response(httpx.Response(503, json=body), "unavailable")
    assert not error.terminal


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
