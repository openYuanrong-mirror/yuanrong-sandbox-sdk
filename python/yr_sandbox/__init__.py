from .scheduling import LabelOperator, ScheduleAffinity
from .types import (
    CommandInfo,
    CommandResult,
    ConnectionConfig,
    DNSPolicy,
    DNSRule,
    EntryInfo,
    Mount,
    NetworkPolicy,
    NetworkRule,
    NodeInfo,
    PauseResult,
    PortForwarding,
    PortRange,
    S3Config,
    SandboxInfo,
    SnapshotInfo,
    ResumeResult,
    TrafficPolicy,
)

SDK_CAPABILITIES = frozenset(
    {
        "local-close",
        "explicit-connection-config",
        "tunnel-proxy-port",
    }
)

__all__ = [
    # current API
    "Sandbox",
    "Shell",
    "Shells",
    "CommandHandle",
    "Pty",
    "PtySession",
    "PtyError",
    "SandboxError",
    # data types
    "LabelOperator",
    "ScheduleAffinity",
    "ConnectionConfig",
    "EntryInfo",
    "CommandResult",
    "CommandInfo",
    "SandboxInfo",
    "SnapshotInfo",
    "PauseResult",
    "ResumeResult",
    "Mount",
    "NetworkPolicy",
    "NetworkRule",
    "PortRange",
    "TrafficPolicy",
    "DNSPolicy",
    "DNSRule",
    "S3Config",
    "PortForwarding",
    "NodeInfo",
    "resources",
    "SDK_CAPABILITIES",
]

# Heavy modules are lazy-loaded so lightweight entry points (the yr-sandbox CLI)
# don't pay for the httpx/websockets import up front.
_lazy_imports = {
    "Sandbox": ".sandbox_api",
    "SandboxError": "._transport",
    "Shell": ".shell",
    "Shells": ".shell",
    "CommandHandle": ".commands",
    "Pty": ".pty",
    "PtySession": ".pty",
    "PtyError": ".pty",
    "PortForwarding": ".types",
    "NodeInfo": ".types",
    "CommandInfo": ".types",
    "NetworkPolicy": ".types",
    "NetworkRule": ".types",
    "PortRange": ".types",
    "TrafficPolicy": ".types",
    "DNSPolicy": ".types",
    "DNSRule": ".types",
    "resources": "._resources",
}


def __getattr__(name):
    module_path = _lazy_imports.get(name)
    if module_path is not None:
        import importlib

        module = importlib.import_module(module_path, __package__)
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
