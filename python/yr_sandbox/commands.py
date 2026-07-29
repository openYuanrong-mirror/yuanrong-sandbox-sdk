"""Command execution helpers for sandbox v1.

Commands map to the RRT ``process.*`` actions:

    process.exec        -> {stdout, stderr, exit_code}
    process.start       -> {pid, error}   (``stdin=True`` opt-in)
    process.poll        -> {status, stdout, stderr, exit_code}
    process.wait        -> {stdout, stderr, exit_code}
    process.kill        -> {killed, error}
    process.send_stdin  -> {error}

Long timeouts use a start+poll loop so individual HTTP calls stay short and
survive gateway idle-connection resets. Real-time stdout streaming is not
exposed yet; invoke-based stdin/background execution is supported.
"""

import logging
import random
import time
from typing import Dict, List, Optional, Union

from ._transport import SandboxClient
from .types import CommandInfo, CommandResult

logger = logging.getLogger(__name__)

_POLL_THRESHOLD = 30  # seconds; above this, switch to start+poll
_POLL_INTERVAL = 10  # seconds per poll call


def _poll_pid_until_done(
    client: SandboxClient, sid: str, pid: int, timeout: int
) -> CommandResult:
    """Poll a running pid until it finishes or the wall-clock deadline expires."""
    deadline = time.monotonic() + timeout
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break

        jittered = _POLL_INTERVAL * (0.7 + random.random() * 0.6)  # noqa: S311 — poll jitter, not crypto
        poll_wait = min(jittered, remaining)
        try:
            poll = client.invoke(
                sid,
                "process.poll",
                {"pid": pid, "wait_timeout": poll_wait},
                timeout=int(poll_wait),
            )
        except Exception as e:
            logger.warning("process.poll failed (pid=%d): %s", pid, e)
            continue

        status = poll["status"]
        if status == "done":
            return CommandResult(
                stdout=poll["stdout"],
                stderr=poll["stderr"],
                exit_code=poll["exit_code"],
            )
        if status == "error":
            return CommandResult(
                stdout="", stderr=poll.get("error", "Unknown error"), exit_code=-1
            )
        # status == "running" → loop

    try:
        client.invoke(sid, "process.kill", {"pid": pid})
    except Exception as e:
        logger.warning("process.kill after timeout failed (pid=%d): %s", pid, e)
    return CommandResult(
        stdout="", stderr=f"Command timed out after {timeout} seconds", exit_code=-1
    )


class CommandHandle:
    """Handle for a background process running in the sandbox."""

    def __init__(self, pid: int, client: SandboxClient, sandbox_id: str):
        self.pid = pid
        self._client = client
        self._sid = sandbox_id

    def wait(self, timeout: Optional[int] = None) -> CommandResult:
        if timeout is None:
            result = self._client.invoke(
                self._sid,
                "process.wait",
                {"pid": self.pid, "timeout": None},
                timeout=-1,
            )
            return CommandResult(
                stdout=result["stdout"],
                stderr=result["stderr"],
                exit_code=result["exit_code"],
            )
        return _poll_pid_until_done(self._client, self._sid, self.pid, timeout)

    def kill(self) -> bool:
        return self._client.invoke(self._sid, "process.kill", {"pid": self.pid})[
            "killed"
        ]

    def send_stdin(self, data: str, eof: bool = False) -> None:
        """Write *data* to the process's stdin (RRT ``cmd_send_stdin``).

        The background process must have been started with ``stdin=True``.
        ``eof=True`` closes stdin so the child sees EOF on its next read.
        """
        result = self._client.invoke(
            self._sid,
            "process.send_stdin",
            {"pid": self.pid, "data": data, "eof": eof},
        )
        if result.get("error"):
            raise RuntimeError(f"Failed to send stdin: {result['error']}")

    def close_stdin(self) -> None:
        self.send_stdin("", eof=True)


class Commands:
    """Client-side wrapper for command execution on the remote sandbox."""

    def __init__(
        self,
        client: SandboxClient,
        sandbox_id: str,
        default_cwd: Optional[str] = None,
    ):
        self._client = client
        self._sid = sandbox_id
        self._default_cwd = default_cwd

    def run(
        self,
        cmd: str,
        background: bool = False,
        envs: Optional[Dict[str, str]] = None,
        cwd: Optional[str] = None,
        timeout: int = 60,
        stdin: bool = False,
    ) -> Union[CommandResult, CommandHandle]:
        """Execute *cmd* on the sandbox.

        ``background=True`` returns a :class:`CommandHandle`. ``stdin=True``
        (only with ``background=True``) keeps an open stdin PIPE so
        ``send_stdin`` can feed the process; otherwise stdin is /dev/null.
        """
        if stdin and not background:
            raise ValueError("stdin is only supported when background=True")
        effective_cwd = cwd if cwd is not None else self._default_cwd

        if background:
            result = self._client.invoke(
                self._sid,
                "process.start",
                {
                    "cmd": cmd,
                    "envs": envs,
                    "cwd": effective_cwd,
                    "want_stdin": stdin,
                },
            )
            if result.get("error"):
                raise RuntimeError(f"Failed to start command: {result['error']}")
            return CommandHandle(result["pid"], self._client, self._sid)

        if timeout > _POLL_THRESHOLD:
            return self._run_with_poll(
                cmd, envs=envs, cwd=effective_cwd, timeout=timeout
            )

        result = self._client.invoke(
            self._sid,
            "process.exec",
            {
                "cmd": cmd,
                "envs": envs,
                "cwd": effective_cwd,
                "timeout": timeout,
            },
            timeout=timeout,
        )
        return CommandResult(
            stdout=result["stdout"],
            stderr=result["stderr"],
            exit_code=result["exit_code"],
        )

    def _run_with_poll(
        self,
        cmd: str,
        envs: Optional[Dict[str, str]] = None,
        cwd: Optional[str] = None,
        timeout: int = 60,
    ) -> CommandResult:
        result = self._client.invoke(
            self._sid, "process.start", {"cmd": cmd, "envs": envs, "cwd": cwd}
        )
        if result.get("error"):
            raise RuntimeError(f"Failed to start command: {result['error']}")
        return _poll_pid_until_done(self._client, self._sid, result["pid"], timeout)

    def list(self) -> List[CommandInfo]:
        processes = self._client.invoke(self._sid, "process.list", {}).get("processes")
        if not isinstance(processes, list):
            return []
        out: List[CommandInfo] = []
        for item in processes:
            if not isinstance(item, dict):
                continue
            running = item.get("running")
            if not isinstance(running, bool):
                running = item.get("status") == "running"
            out.append(
                CommandInfo(
                    pid=int(item.get("pid", 0)),
                    command=str(item.get("cmd", "")),
                    running=running,
                )
            )
        return out

    def kill(self, pid: int) -> bool:
        return self._client.invoke(self._sid, "process.kill", {"pid": pid})["killed"]

    def send_stdin(self, pid: int, data: str, eof: bool = False) -> None:
        """Write *data* to the stdin of process *pid* (RRT ``cmd_send_stdin``)."""
        result = self._client.invoke(
            self._sid, "process.send_stdin", {"pid": pid, "data": data, "eof": eof}
        )
        if result.get("error"):
            raise RuntimeError(f"Failed to send stdin: {result['error']}")

    def close_stdin(self, pid: int) -> None:
        self.send_stdin(pid, "", eof=True)
