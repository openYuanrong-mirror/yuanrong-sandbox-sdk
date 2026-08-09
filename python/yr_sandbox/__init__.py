from .types import (
    CommandInfo,
    CommandResult,
    EntryInfo,
    Mount,
    NetworkPolicy,
    NodeInfo,
    PortForwarding,
    S3Config,
    SandboxInfo,
)

SDK_CAPABILITIES = frozenset(
    {
        "local-close",
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
    # data types
    "EntryInfo",
    "CommandResult",
    "CommandInfo",
    "SandboxInfo",
    "Mount",
    "NetworkPolicy",
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
