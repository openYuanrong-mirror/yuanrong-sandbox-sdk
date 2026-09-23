"""Command waits stop on client closure and pace failed requests."""

import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest
from yr_sandbox import _http_pool, commands
from yr_sandbox._http_pool import SandboxClientClosedError
from yr_sandbox._transport import SandboxClient, SandboxError, SandboxHTTPError
from yr_sandbox.commands import (
    CommandHandle,
    CommandNotFound,
    Commands,
    CommandUnavailable,
    UnsupportedFeature,
)
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


@pytest.mark.parametrize("options,expected_timeout", [
    ({}, None), ({"timeout": None}, None), ({"timeout": 60}, 60),
    ({"timeout": 2}, 2), ({"timeout": 0}, 0),
])
def test_background_execution_deadline_is_explicit(options, expected_timeout):
    client = Mock(spec=SandboxClient)
    client.invoke.side_effect = [
        {"protocol_version": 1, "capabilities": [
            "stable-command-id", "recoverable-command-result",
        ]},
        {"pid": 42},
    ]
    result = Commands(client, "sandbox").run(
        "sleep 120", background=True, command_id="cmd-deadline", **options,
    )
    assert result.command_id == "cmd-deadline"
    call = client.invoke.call_args
    assert call.args[:2] == ("sandbox", "process.start")
    request = call.args[2]
    assert request["command_id"] == "cmd-deadline"
    if expected_timeout is None:
        assert "timeout" not in request
    else:
        assert request["timeout"] == expected_timeout


@pytest.mark.parametrize("options,expected_timeout", [({}, 60), ({"timeout": 90}, 90)])
def test_foreground_default_deadline_is_preserved(monkeypatch, options, expected_timeout):
    collection = Commands(Mock(spec=SandboxClient), "sandbox")
    poll = Mock(return_value=CommandResult("done", "", 0))
    monkeypatch.setattr(collection, "_run_with_poll", poll)
    collection.run("sleep 120", **options)
    poll.assert_called_once_with("sleep 120", None, None, expected_timeout)


def test_wait_uses_recoverable_process_wait_with_connection():
    client = Mock(spec=SandboxClient)
    client._connection = object()
    client.invoke.side_effect = [
        {"command_id": "cmd-42", "pid": 42, "status": "RUNNING"},
        {
            "command_id": "cmd-42",
            "pid": 42,
            "status": "SUCCEEDED",
            "stdout": "done",
            "stderr": "",
            "exit_code": 0,
        },
    ]

    result = CommandHandle("cmd-42", client, "sandbox", 42).wait(timeout=15)

    assert result.status == CommandStatus.SUCCEEDED
    assert result.stdout == "done"
    assert [call.args[1] for call in client.invoke.call_args_list] == [
        "process.get",
        "process.wait",
    ]
    wait_call = client.invoke.call_args_list[1]
    assert wait_call.args[2] == {"command_id": "cmd-42", "timeout": 15}
    assert wait_call.kwargs["timeout"] == 16


@pytest.mark.parametrize("as_http_error", [False, True])
def test_wait_timeout_returns_running_result(as_http_error):
    client = Mock(spec=SandboxClient)
    timeout = {
        "status": "running",
        "error_code": "WAIT_TIMEOUT",
        "error": "command wait timed out",
    }
    response = SandboxHTTPError(400, timeout, timeout["error"])
    client.invoke.side_effect = [
        {"command_id": "cmd-42", "pid": 42, "status": "RUNNING"},
        response if as_http_error else timeout,
    ]

    result = CommandHandle("cmd-42", client, "sandbox", 42).wait(timeout=15)

    assert result.status == CommandStatus.RUNNING
    assert result.error_code == "WAIT_TIMEOUT"
    assert result.error_message == "command wait timed out"
    assert result.exit_code is None


def test_wait_async_returns_timeout_then_can_wait_again():
    client = Mock(spec=SandboxClient)
    client.invoke.side_effect = [
        {"command_id": "cmd-42", "status": "RUNNING"},
        {"status": "running", "error_code": "WAIT_TIMEOUT", "error": "still running"},
        {"command_id": "cmd-42", "status": "SUCCEEDED", "exit_code": 0},
    ]
    handle = CommandHandle("cmd-42", client, "sandbox", 42)

    timeout = asyncio.run(handle.wait_async(timeout=0))
    completed = handle.wait(timeout=15)

    assert timeout.status == CommandStatus.RUNNING
    assert timeout.error_code == "WAIT_TIMEOUT"
    assert timeout.error_message == "still running"
    assert completed.status == CommandStatus.SUCCEEDED
    assert completed.exit_code == 0


def test_legacy_wait_timeout_returns_running_result():
    client = Mock(spec=SandboxClient)
    client.invoke.return_value = {"status": "running"}

    result = CommandHandle(
        "cmd-42", client, "sandbox", 42, recoverable=False
    ).wait(timeout=15)

    assert result.status == CommandStatus.RUNNING
    assert result.error_code == "WAIT_TIMEOUT"
    assert result.exit_code is None
    client.invoke.assert_called_once_with(
        "sandbox", "process.poll", {"pid": 42, "wait_timeout": 15}, timeout=16
    )


@pytest.mark.parametrize(
    "capability_response",
    [
        {},
        SandboxHTTPError(400, {"error": "unknown action"}, "unknown action"),
    ],
)
def test_background_command_falls_back_to_legacy_pid_protocol(capability_response):
    client = Mock(spec=SandboxClient)
    client.invoke.side_effect = [
        capability_response,
        {"pid": 42},
        {"status": "done", "stdout": "done", "stderr": "", "exit_code": 0},
    ]

    handle = Commands(client, "sandbox").run("echo done", background=True)
    result = handle.wait(timeout=15)

    assert result.status == CommandStatus.SUCCEEDED
    assert result.stdout == "done"
    assert [call.args[1] for call in client.invoke.call_args_list] == [
        "process.capabilities",
        "process.start",
        "process.poll",
    ]
    start = client.invoke.call_args_list[1]
    assert start.args[2] == {
        "cmd": "echo done",
        "envs": None,
        "cwd": None,
        "want_stdin": False,
    }
    wait = client.invoke.call_args_list[2]
    assert wait.args[2] == {"pid": 42, "wait_timeout": 15}


def test_long_foreground_command_falls_back_to_legacy_pid_protocol():
    client = Mock(spec=SandboxClient)
    client.invoke.side_effect = [
        {},
        {"pid": 42},
        {"status": "running"},
        {"status": "done", "stdout": "done", "stderr": "", "exit_code": 0},
    ]

    result = Commands(client, "sandbox").run("sleep 31", timeout=31)

    assert result.status == CommandStatus.SUCCEEDED
    assert [call.args[1] for call in client.invoke.call_args_list] == [
        "process.capabilities",
        "process.start",
        "process.poll",
        "process.poll",
    ]
    assert "timeout" not in client.invoke.call_args_list[1].args[2]


def test_legacy_handle_uses_pid_for_wait_kill_and_stdin():
    client = Mock(spec=SandboxClient)
    client.invoke.side_effect = [
        {},
        {"pid": 42},
        {"stdout": "done", "stderr": "", "exit_code": 0},
        {"killed": True},
        {},
    ]

    handle = Commands(client, "sandbox").run("cat", background=True, stdin=True)

    assert handle.wait().status == CommandStatus.SUCCEEDED
    assert handle.kill()
    handle.send_stdin("input", eof=True)
    assert [call.args[1] for call in client.invoke.call_args_list] == [
        "process.capabilities",
        "process.start",
        "process.wait",
        "process.kill",
        "process.send_stdin",
    ]
    assert client.invoke.call_args_list[2].args[2] == {"pid": 42, "timeout": None}
    assert client.invoke.call_args_list[3].args[2] == {"pid": 42}
    assert client.invoke.call_args_list[4].args[2] == {
        "pid": 42,
        "data": "input",
        "eof": True,
    }


@pytest.mark.parametrize("entry_point", ["handle", "collection"])
@pytest.mark.parametrize("code,status", [
    ("COMMAND_NOT_FOUND", 404),
    ("COMMAND_NOT_RUNNING", 400),
])
def test_kill_absent_command_is_successful_noop(entry_point, code, status):
    client = Mock(spec=SandboxClient)
    client.invoke.side_effect = SandboxHTTPError(
        status, {"error_code": code, "killed": False}, code
    )

    if entry_point == "handle":
        killed = CommandHandle("cmd-42", client, "sandbox", 42).kill()
    else:
        killed = Commands(client, "sandbox").kill("cmd-42")

    assert killed is False
    client.invoke.assert_called_once_with(
        "sandbox", "process.kill", {"command_id": "cmd-42"}
    )


def test_kill_signal_failure_still_raises():
    client = Mock(spec=SandboxClient)
    client.invoke.return_value = {
        "killed": False,
        "error_code": "SIGNAL_FAILED",
        "error": "permission denied",
    }

    with pytest.raises(SandboxError, match="permission denied"):
        Commands(client, "sandbox").kill("cmd-42")


@pytest.mark.parametrize("status", ["NOT_FOUND", "ALREADY_EXITED"])
def test_kill_noop_response_returns_false(status):
    client = Mock(spec=SandboxClient)
    client.invoke.return_value = {"status": status, "killed": False, "error": None}

    assert Commands(client, "sandbox").kill("cmd-42") is False


def test_explicit_command_id_requires_recoverable_protocol():
    client = Mock(spec=SandboxClient)
    client.invoke.return_value = {}

    with pytest.raises(UnsupportedFeature, match="recovery protocol v1"):
        Commands(client, "sandbox").run(
            "true", background=True, command_id="stable-id"
        )

    client.invoke.assert_called_once_with("sandbox", "process.capabilities", {})


def test_capability_transport_failure_does_not_downgrade_protocol():
    client = Mock(spec=SandboxClient)
    error = SandboxHTTPError(503, {"error": "unavailable"}, "unavailable")
    client.invoke.side_effect = error

    with pytest.raises(UnsupportedFeature, match="failed to negotiate") as raised:
        Commands(client, "sandbox").run("true", background=True)

    assert raised.value.__cause__ is error
    client.invoke.assert_called_once_with("sandbox", "process.capabilities", {})


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
    SandboxError("gateway unavailable"), CommandUnavailable("watch disconnected"),
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
        raise httpx.ReadError("request failed")

    handle.wait.side_effect = wait
    result = collection._run_with_poll("sleep 60", None, None, 2)
    assert result.status == CommandStatus.TIMED_OUT
    assert clock.now == pytest.approx(2)
    assert clock.sleeps == pytest.approx([1, 0.8])
    assert handle.wait.call_count == 2
    handle.kill.assert_called_once_with()


def test_failure_after_deadline_does_not_delay(clock, running):
    collection, handle = running

    def wait(_timeout):
        clock.now += 3
        raise httpx.ReadError("request failed")

    handle.wait.side_effect = wait
    result = collection._run_with_poll("sleep 60", None, None, 3)
    assert result.status == CommandStatus.TIMED_OUT
    assert clock.sleeps == []
    assert handle.wait.call_count == 1
    handle.kill.assert_called_once_with()


def test_wait_timeout_continues_waiting(clock, running):
    collection, handle = running
    expected = CommandResult("done", "", 0)
    timeout = CommandResult(
        "", "", None, status=CommandStatus.RUNNING, error_code="WAIT_TIMEOUT"
    )
    handle.wait.side_effect = [timeout, expected]
    assert collection._run_with_poll("sleep 60", None, None, 60) is expected
    assert clock.sleeps == []
    handle.kill.assert_not_called()


@pytest.mark.parametrize("terminal", [
    CommandResult("partial", "deadline exceeded", None, status=CommandStatus.TIMED_OUT),
    CommandResult("finished", "", 0, status=CommandStatus.SUCCEEDED),
])
def test_local_deadline_racing_remote_completion_returns_terminal_result(clock, running, terminal):
    collection, handle = running
    waits = []

    def wait(timeout):
        waits.append(timeout)
        if len(waits) == 1:
            clock.now += 3
            raise TimeoutError("notification wait expired")
        assert timeout == 0
        return terminal

    handle.wait.side_effect = wait
    handle.kill.side_effect = SandboxHTTPError(
        400, {"error_code": "COMMAND_NOT_RUNNING", "killed": False}, "already terminal",
    )
    assert collection._run_with_poll("sleep 3", None, None, 3) is terminal
    handle.kill.assert_called_once_with()
    assert len(waits) == 2


def test_local_deadline_returns_terminal_result_after_noop_kill(clock, running):
    collection, handle = running
    terminal = CommandResult("done", "", 0, status=CommandStatus.SUCCEEDED)

    def wait(timeout):
        if timeout:
            clock.now += 3
            return CommandResult(
                "", "", None, status=CommandStatus.RUNNING, error_code="WAIT_TIMEOUT"
            )
        return terminal

    handle.wait.side_effect = wait
    handle.kill.return_value = False

    assert collection._run_with_poll("sleep 3", None, None, 3) is terminal
    assert handle.wait.call_count == 2
    handle.kill.assert_called_once_with()


@pytest.mark.parametrize("code,payload", [(401, {}), (400, {"error_code": "INVALID_COMMAND_ID"})])
def test_local_deadline_preserves_other_kill_errors(clock, running, code, payload):
    collection, handle = running
    error = SandboxHTTPError(code, payload, "kill rejected")
    handle.kill.side_effect = error

    def wait(_timeout):
        clock.now += 3
        raise TimeoutError("notification wait expired")

    handle.wait.side_effect = wait
    with pytest.raises(SandboxHTTPError) as raised:
        collection._run_with_poll("sleep 3", None, None, 3)
    assert raised.value is error
    assert handle.wait.call_count == 1


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
    collection = Commands(client, "sandbox")
    collection._capabilities_checked = True
    try:
        with pytest.raises(SandboxHTTPError) as raised:
            if entry_point == "wait":
                CommandHandle("cmd-42", client, "sandbox").wait(timeout=7200)
            else:
                collection.run("sleep 60", timeout=7200)
        assert raised.value.terminal
        assert raised.value.payload == body
        assert raised.value.status_code == http_status
        assert len(requests) == (2 if entry_point == "run" else 1)
        assert all(request.method == "POST" for request in requests)
        assert clock.sleeps == []
        assert raised.value.request_id == requests[-1].headers["X-YR-Request-ID"]
        assert "command wait failed" not in caplog.text
    finally:
        client.close()
        registry.close_all()


def test_recovering_response_retries(clock, running):
    collection, handle = running
    error = SandboxHTTPError(503, {
        "code": "SANDBOX_RECOVERING", "state": "FAILED", "retryable": True,
    }, "recovering")
    expected = CommandResult("done", "", 0)
    handle.wait.side_effect = [error, expected]
    assert collection._run_with_poll("sleep 60", None, None, 60) is expected
    assert clock.sleeps == [1]
    handle.kill.assert_not_called()


@pytest.mark.parametrize("error", [
    httpx.ReadError("reset"), CommandUnavailable("watch disconnected"),
    SandboxError("gateway unavailable", request_id="request-42"),
])
def test_wait_failures_are_bounded(clock, running, error, caplog):
    collection, handle = running
    handle.wait.side_effect = error
    with pytest.raises(CommandUnavailable, match="after 3 consecutive errors") as raised:
        collection._run_with_poll("sleep 7200", None, None, 7200)
    assert raised.value.__cause__ is error
    assert raised.value.sandbox_id == "sandbox"
    assert raised.value.command_id == "cmd-42"
    assert raised.value.request_id == getattr(error, "request_id", None)
    assert handle.wait.call_count == 3
    assert clock.sleeps == [1, 1]
    assert caplog.text.count("command wait failed") == 2
    handle.kill.assert_not_called()


def test_normal_wait_timeout_resets_error_budget(clock, running):
    collection, handle = running
    error = httpx.ReadError("reset")
    expected = CommandResult("done", "", 0)
    handle.wait.side_effect = [error, error, TimeoutError("still running"), error, error, expected]
    assert collection._run_with_poll("sleep 60", None, None, 60) is expected
    assert clock.sleeps == [1, 1, 1, 1]
    handle.kill.assert_not_called()


@pytest.mark.parametrize("error", [
    SandboxHTTPError(401, {}, "unauthorized"),
    SandboxHTTPError(403, {}, "forbidden"),
    SandboxHTTPError(400, {}, "bad request"),
    CommandNotFound("missing command"),
    RuntimeError("unexpected error"), ValueError("bad data"),
])
def test_nonretryable_wait_error_escapes(clock, running, error):
    collection, handle = running
    handle.wait.side_effect = error
    with pytest.raises(type(error)) as raised:
        collection._run_with_poll("sleep 60", None, None, 60)
    assert raised.value is error
    assert handle.wait.call_count == 1
    assert clock.sleeps == []
    handle.kill.assert_not_called()
