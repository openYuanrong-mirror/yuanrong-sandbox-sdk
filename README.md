# openYuanrong Sandbox SDKs

This repository is the multi-language SDK workspace for openYuanrong remote
sandboxes.  The Python SDK is implemented today; Go, Rust, and Java are reserved
as first-class SDK directories so future clients can share the same repository,
release process, examples policy, and protocol vocabulary.

## Layout

| Path | Status | Purpose |
| --- | --- | --- |
| `python/` | implemented | Python package `openyuanrong-sandbox`, exposing `yr_sandbox.Sandbox` and the CLI entrypoint. |
| `go/` | planned | Future Go SDK module. Keep Go-specific code, examples, and tests here. |
| `rust/` | planned | Future Rust crate. Keep Rust-specific code, examples, and tests here. |
| `java/` | planned | Future Java/Maven or Gradle SDK. Keep Java-specific code, examples, and tests here. |
| `build.sh` | root build entrypoint | Builds the Python SDK by default for existing CI/release callers. |

## Implemented SDK

See [`python/README.md`](python/README.md) for install/configuration details and
runnable examples.

Quick build from the workspace root:

```bash
PYTHON=python3 bash build.sh /tmp/openyuanrong-sandbox-dist
```

Equivalent Python-only build:

```bash
cd python
PYTHON=python3 bash build.sh /tmp/openyuanrong-sandbox-dist
```

## Cross-language conventions

All language SDKs should keep the same user-facing concepts:

- `Sandbox` lifecycle: create, command execution, filesystem operations, kill.
- Python callers may pass an explicit `ConnectionConfig`; all SDKs retain
  environment fallback through `YR_SERVER_ADDRESS`, `YR_TOKEN`, optional
  `YR_GATEWAY_ADDRESS`, and TLS flags.
- Frontend control plane: `/api/sandbox/v1/sandboxes`.
- Direct file and action data plane through `/direct/...`, plus reverse tunnel
  access through `/tunnel/...`.
- Runnable examples only. Infra-specific or nonportable demos should live in docs
  or private test fixtures, not in public SDK example directories.

## HTTP RESTful API contract

All language SDKs should target the current frontend HTTP/WS contract instead
of exposing runtime-internal ports to users. The detailed platform reference is
maintained in the main yuanrong workspace at `docs/features/sandbox-rest-api.md`.

### Environment and auth

The Python SDK accepts the same values as an immutable `ConnectionConfig` on
`Sandbox`, `Sandbox.delete`, and `resources`. Explicit configuration is kept on
the SDK objects and does not read or modify process-global `YR_*` connection
variables. Omitting it preserves the environment-based behavior below.

| Setting | Meaning |
| --- | --- |
| `YR_SERVER_ADDRESS` | Frontend gateway `host:port`. Used for lifecycle, invoke, and `/direct` file IO. |
| `YR_TOKEN` | JWT sent as raw `X-Auth: <token>` on authenticated frontend routes. Do not use `Authorization: Bearer`. |
| `YR_TLS` | `1/true/yes` selects `https://` for frontend routes; `0/false/no` selects plaintext HTTP. |
| `YR_GATEWAY_ADDRESS` | Optional sandbox gateway/router host for reverse tunnel and user port URLs; falls back to `YR_SERVER_ADDRESS`. |
| `YR_GATEWAY_TLS` | `1/true/yes` selects `wss://` for `/tunnel`; default is plaintext `ws://`. |

### Control plane

Base path: `/api/sandbox/v1/sandboxes` on `YR_SERVER_ADDRESS`.

| Method | Path | Body / query | Result |
| --- | --- | --- | --- |
| `POST` | `/api/sandbox/v1/sandboxes` | `CreateV1Request` JSON | `{sandboxId, instanceId, status, tunnel?}` |
| `DELETE` | `/api/sandbox/v1/sandboxes/{sandboxID}` | none | idempotent teardown; `404` is treated as already deleted by the SDK |
| `POST` | `/api/sandbox/v1/sandboxes/{sandboxID}/invoke` | `{"action": string, "args": object}` | action result JSON |

`CreateV1Request` fields used by SDKs include `name`, `namespace`, `tenant`,
`runtime`, `image`/`rootfs`, `ports`, `idleTimeoutSeconds`,
`createTimeoutSeconds`, `scheduleTimeoutSeconds`, `cpu`, `memory`,
`cpu_limit`, `mem_limit`, `storage_limit`, `env`, `mounts`, `extra_config`, and
`tunnel`.
Frontend owns internal RRT port environment injection (`RRT_HTTP_PORT`,
`RRT_TUNNEL_WS_PORT`, `RRT_TUNNEL_HTTP_PORT`); SDK callers should request
features declaratively instead of setting those ports.

Create and schedule timeouts use seconds. Callers normally set only one:
`scheduleTimeoutSeconds = createTimeoutSeconds - 30`, or
`createTimeoutSeconds = scheduleTimeoutSeconds + 30`. If both are sent, the
schedule timeout must not exceed the create timeout and the difference must be
at least 30 seconds.

### Direct data plane

SDKs should prefer the frontend `/direct` aliases and fall back to frontend
`/invoke` only when `/direct` is unavailable. The frontend authenticates the
request, strips platform credentials before proxying to sandboxRouter/RRT, and
hides the internal RRT control port from user URLs.

| Method | Path | Body / query | Use |
| --- | --- | --- | --- |
| `POST` | `/direct/{safeID}/invoke` | `{"action": string, "args": object}` | low-latency command/fs/shell action invoke |
| `GET` | `/direct/{safeID}/healthz` | none | RRT health probe |
| `POST` | `/direct/{safeID}/upload?path=<abs>&type=file|tar` | raw bytes or tar stream | binary upload / directory upload |
| `GET` | `/direct/{safeID}/download?path=<abs>&type=file|tar` | none | binary download / directory download |

`safeID` is the router-safe form of `sandboxID`. The old explicit-port form
`/direct/{safeID}/{rrtPort}/...` is a frontend compatibility alias only; new
SDKs should not expose it.

### Tunnel and user ports

| Surface | URL shape | Notes |
| --- | --- | --- |
| Reverse tunnel | `/tunnel/{safeID}` | SDK connects a local upstream to the gateway; default is plaintext `ws://` and does not send `YR_TOKEN`. |
| User port forwarding | `http://<gateway>/<safeID>/<port>` | Returned by SDK `get_port_url(port)` for ports requested at create time. User service ports are public at router layer. |

The shared action envelope is always `{"action": <name>, "args": {...}}`.
Supported action names include process (`process.exec`, `process.start`,
`process.poll`, `process.wait`, `process.kill`, `process.send_stdin`), file
(`file.read`, `file.write`, `file.list`, `file.exists`, `file.remove`,
`file.rename`, `file.mkdir`, `file.stat`), and shell (`shell.create`,
`shell.run`, `shell.poll`, `shell.delete`) operations.

## Adding another language SDK

When adding Go/Rust/Java support:

1. Put language-native package metadata under that language directory only.
2. Add a language-local `README.md`, `examples/`, and `tests/`.
3. Reuse the protocol names and environment variables above.
4. Add CI/build steps explicitly for that language; do not overload the Python
   package build.
5. Keep root `build.sh` stable unless the release pipeline is updated deliberately.
