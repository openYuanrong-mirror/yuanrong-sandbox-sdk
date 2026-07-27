from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional

# Default action timeout in seconds.
YR_GET_DEFAULT_TIMEOUT = 300

# Extra seconds added to the user-specified timeout for the RPC call,
# to account for network overhead and serialization.
YR_GET_TIMEOUT_BUFFER = 30


@dataclass(frozen=True)
class PortForwarding:
    """Port-forwarding descriptor.

    Port forwarding is requested at sandbox creation time. The SDK builds
    router URLs as ``http://<gateway>/<safeID>/<port>`` through
    :meth:`yr_sandbox.Sandbox.get_port_url`.
    """

    port: int
    protocol: str = "TCP"


@dataclass(frozen=True)
class EntryInfo:
    name: str
    path: str
    type: str  # "file" | "dir" | "symlink"
    size: int
    permissions: str
    modified_time: float


@dataclass(frozen=True)
class CommandResult:
    stdout: str
    stderr: str
    exit_code: int


@dataclass(frozen=True)
class SandboxInfo:
    id: str
    state: str  # "running" | "stopped"
    cpu: Optional[int]
    memory: Optional[int]
    image: Optional[str]


@dataclass(frozen=True)
class S3Config:
    """S3 object storage configuration."""

    endpoint: str
    bucket: str
    object: str
    access_key: Optional[str] = None
    secret_key: Optional[str] = field(default=None, repr=False)

    def __post_init__(self) -> None:
        for field_name in ("endpoint", "bucket", "object"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "endpoint": self.endpoint,
            "bucket": self.bucket,
            "object": self.object,
        }
        if self.access_key is not None:
            d["accessKey"] = self.access_key
        if self.secret_key is not None:
            d["secretKey"] = self.secret_key
        return d


@dataclass(frozen=True)
class Mount:
    """Read-only mount configuration for Sandbox.

    Mounts are always read-only. The source is either a container image
    (``image_url``) or an S3 object (``s3_config``); sandboxd resolves
    the source to a local path and exposes it at ``target``.

    ``type`` selects the in-sandbox filesystem:

    - ``"bind"`` (default): bind-mount the resolved host path (file or
      directory tree) at ``target`` via FDFS.
    - ``"erofs"``: mount the resolved host path as a read-only EROFS
      filesystem. The source must point at an EROFS image file (e.g.
      an S3 object whose content is a ``.img`` EROFS image).

    Exactly one of ``image_url`` or ``s3_config`` must be specified.

    Examples::

        Mount(target="/opt/tool", image_url="registry/tool:v1")
        Mount(target="/weights", type="erofs", s3_config=S3Config(...))
    """

    target: str
    image_url: Optional[str] = None
    s3_config: Optional[S3Config] = None
    type: str = "bind"

    def __post_init__(self) -> None:
        if not isinstance(self.target, str) or not self.target.startswith("/"):
            raise ValueError("target must be an absolute sandbox path")
        sources = [self.image_url, self.s3_config]
        count = sum(1 for s in sources if s is not None)
        if count != 1:
            raise ValueError(
                f"Exactly one of image_url, s3_config must be specified, got {count}"
            )
        if self.image_url is not None and (
            not isinstance(self.image_url, str) or not self.image_url.strip()
        ):
            raise ValueError("image_url must be a non-empty string")
        if self.type not in ("bind", "erofs"):
            raise ValueError(f"type must be 'bind' or 'erofs', got {self.type!r}")

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "type": self.type,
            "target": self.target,
            "options": ["ro"],
        }
        if self.image_url is not None:
            d["image_url"] = self.image_url
        if self.s3_config is not None:
            d["s3_config"] = self.s3_config.to_dict()
        return d


@dataclass(frozen=True)
class NodeInfo:
    id: str
    status: int
    capacity: Mapping[str, float]
    allocatable: Mapping[str, float]
    labels: Mapping[str, Any]


@dataclass(frozen=True)
class CommandInfo:
    pid: int
    command: str
    running: bool
