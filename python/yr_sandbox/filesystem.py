"""Filesystem helpers for sandbox v1.

Small file operations use frontend invoke actions. Binary file and directory
copy paths prefer the frontend `/direct` route so large payloads avoid JSON
base64 envelopes.
"""

import os
import tarfile
import tempfile
import threading
from typing import Iterator, List, Literal, Union, overload

from ._transport import SandboxClient
from .types import EntryInfo


def _safe_extract_tar(tar: tarfile.TarFile, dest: str) -> None:
    """Extract a tar archive under ``dest``, rejecting members whose resolved
    path escapes ``dest`` (path-traversal guard; avoids unsafe extractall)."""
    dest_real = os.path.realpath(dest)
    for member in tar.getmembers():
        target = os.path.realpath(os.path.join(dest, member.name))
        if target != dest_real and not target.startswith(dest_real + os.sep):
            raise RuntimeError(
                f"unsafe tar member escapes destination: {member.name!r}"
            )
        tar.extract(member, dest)


def _tar_directory_chunks(
    local_path: str, chunk_size: int = 1024 * 1024
) -> Iterator[bytes]:
    """Stream a directory as tar bytes without materializing a temporary archive."""
    read_fd, write_fd = os.pipe()
    errors = []

    def produce_tar() -> None:
        try:
            with os.fdopen(write_fd, "wb") as writer:
                with tarfile.open(fileobj=writer, mode="w|") as tar:
                    for name in sorted(os.listdir(local_path)):
                        tar.add(os.path.join(local_path, name), arcname=name)
        except BaseException as exc:  # propagate producer failures to consumer
            errors.append(exc)

    producer = threading.Thread(
        target=produce_tar, name="yr-copy-from-local-tar", daemon=True
    )
    producer.start()

    try:
        with os.fdopen(read_fd, "rb") as reader:
            while True:
                chunk = reader.read(chunk_size)
                if not chunk:
                    break
                yield chunk
    finally:
        producer.join()

    if errors:
        raise errors[0]


def _check(result: dict, msg: str) -> dict:
    if result.get("error"):
        raise RuntimeError(f"{msg}: {result['error']}")
    return result


class Filesystem:
    """Client-side wrapper for filesystem operations on the remote sandbox."""

    def __init__(self, client: SandboxClient, sandbox_id: str):
        self._client = client
        self._sid = sandbox_id

    def _invoke(self, action: str, **args) -> dict:
        return self._client.invoke(self._sid, action, args)

    @overload
    def read(self, path: str, format: Literal["text"] = "text") -> str: ...

    @overload
    def read(self, path: str, format: Literal["bytes"]) -> bytes: ...

    def read(self, path: str, format: str = "text") -> Union[str, bytes]:
        if format not in ("text", "bytes"):
            raise ValueError("format must be 'text' or 'bytes'")
        data = self._client.download_bytes_direct(self._sid, path, timeout_or_default())
        if format == "bytes":
            return data
        return data.decode("utf-8", errors="replace")

    def write(self, path: str, data: Union[str, bytes]) -> EntryInfo:
        payload = data if isinstance(data, bytes) else data.encode("utf-8")
        result = self._client.upload_bytes_direct(
            self._sid, payload, path, timeout_or_default()
        )
        return self._entry_from_upload(_check(result, f"Failed to write {path}"))

    def list(self, path: str, depth: int = 1) -> List[EntryInfo]:
        result = _check(
            self._invoke("file.list", path=path, depth=depth),
            f"Failed to list {path}",
        )
        return [
            EntryInfo(
                name=e["name"],
                path=e["path"],
                type=e["type"],
                size=e["size"],
                permissions=e["permissions"],
                modified_time=e["modified_time"],
            )
            for e in result["entries"]
        ]

    def exists(self, path: str) -> bool:
        return self._invoke("file.exists", path=path)["exists"]

    def remove(self, path: str) -> None:
        _check(self._invoke("file.remove", path=path), f"Failed to remove {path}")

    def rename(self, old_path: str, new_path: str) -> EntryInfo:
        result = _check(
            self._invoke("file.rename", old_path=old_path, new_path=new_path),
            f"Failed to rename {old_path} -> {new_path}",
        )
        return self._entry(result)

    def make_dir(self, path: str) -> bool:
        result = _check(
            self._invoke("file.make_dir", path=path), f"Failed to make dir {path}"
        )
        return result["created"]

    def get_info(self, path: str) -> EntryInfo:
        # RRT's normalize_sandbox_action accepts file.stat / file.info /
        # fs.get_info — NOT file.get_info. Use file.stat. (Verified via E2E.)
        result = _check(
            self._invoke("file.stat", path=path),
            f"Failed to get info for {path}",
        )
        return self._entry(result)

    # ── bulk copy via binary direct upload/download ───────────────────

    def copy_from_local(self, local_path: str, remote_path: str) -> None:
        """Copy a local file or directory **into** the sandbox.

        Files use the frontend ``/direct`` binary upload path. Directories are
        packed as a tar archive and uploaded over the same HTTP binary path.
        """
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Local source path not found: {local_path}")
        if os.path.isdir(local_path):
            result = self._client.upload_stream_direct(
                self._sid,
                _tar_directory_chunks(local_path),
                remote_path,
                timeout_or_default(),
                upload_type="tar",
            )
            _check(result, f"Failed to copy {local_path} to {remote_path}")
            return
        result = self._client.upload_file_direct(
            self._sid, local_path, remote_path, timeout_or_default()
        )
        _check(result, f"Failed to copy {local_path} to {remote_path}")

    def copy_to_local(self, remote_path: str, local_path: str) -> None:
        """Copy a file or directory **from** the sandbox to the local machine.

        A remote directory arrives as a single tar archive that is extracted
        under ``local_path``.
        """
        if self.get_info(remote_path).type == "dir":
            os.makedirs(local_path, exist_ok=True)
            fd, tar_path = tempfile.mkstemp(suffix=".tar")
            os.close(fd)
            try:
                self._client.download_file_direct(
                    self._sid,
                    remote_path,
                    tar_path,
                    timeout_or_default(),
                    download_type="tar",
                )
                with tarfile.open(tar_path, "r") as tar:
                    _safe_extract_tar(tar, local_path)
            finally:
                os.unlink(tar_path)
            return
        self._client.download_file_direct(
            self._sid, remote_path, local_path, timeout_or_default()
        )

    @staticmethod
    def _entry(result: dict) -> EntryInfo:
        return EntryInfo(
            name=result["name"],
            path=result["path"],
            type=result["type"],
            size=result["size"],
            permissions=result["permissions"],
            modified_time=result["modified_time"],
        )

    @staticmethod
    def _entry_from_upload(result: dict) -> EntryInfo:
        return EntryInfo(
            name=result.get("name", os.path.basename(result.get("path", ""))),
            path=result["path"],
            type=result.get("type", "file"),
            size=result["size"],
            permissions=result.get("permissions", ""),
            modified_time=result.get("modified_time", 0.0),
        )


def timeout_or_default() -> float:
    return float(os.environ.get("YR_DIRECT_UPLOAD_TIMEOUT", "300"))
