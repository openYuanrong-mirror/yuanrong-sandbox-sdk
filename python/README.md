# openyuanrong-sandbox Python SDK

Python SDK for openYuanrong sandboxes. The transport uses the frontend
**sandbox v1** HTTP interface backed by RRT, plus the gateway WebSocket route
for reverse tunnels.

```python
from yr_sandbox import Sandbox

with Sandbox(image="python:3.12-slim", cpu=2000, memory=4096) as sb:
    sb.files.write("/tmp/hello.txt", "hello world")
    print(sb.commands.run("cat /tmp/hello.txt").stdout)
```

Request whole GPUs with the ``type:model:count`` form:

```python
Sandbox(xpu="gpu:l20:1")
Sandbox(xpu="gpu:h100:2")
Sandbox(xpu="gpu::1")  # any GPU model
Sandbox()  # no XPU request
```

The first version accepts one request, supports only ``gpu``, and requires a
positive whole-device count. Type matching is case-insensitive in the SDK, and
the value is forwarded unchanged; Frontend normalizes the resource type for
FunctionSystem. An empty model keeps the three-field form and lets
FunctionSystem select any model.

Request temporary writable storage:

```python
from yr_sandbox import Sandbox

Sandbox(storage_mb=153600)
```

`storage_mb` is expressed in MiB. Frontend converts it to bytes in the
FunctionSystem custom resource named `storage`.

Normally, configure only one sandbox create budget; the other value is derived
automatically with a 30-second startup buffer:

```python
Sandbox(image="python:3.12-slim", create_timeout=120)   # schedule_timeout=90
Sandbox(image="python:3.12-slim", schedule_timeout=90) # create_timeout=120
```

When both values are configured, `schedule_timeout <= create_timeout`, and their
difference must be at least 30 seconds. The 30-second buffer covers rootfs/image
preparation, runtime startup, and Running-state confirmation.

Configure a non-default sandbox-side reverse-tunnel proxy port when needed:

```python
sb = Sandbox(upstream="127.0.0.1:8000", proxy_port=9001)
assert sb.get_tunnel_url() == "http://127.0.0.1:9001"
```

The frontend derives the WebSocket port as `proxy_port - 1` and owns both
internal port mappings. User `port_forwardings` must not reuse either port.
Call `sb.close()` to release only local SDK resources while keeping the remote
sandbox alive; call `sb.kill()` to also delete a non-detached remote sandbox.

Creation-time network policies are optional:

```python
from yr_sandbox import NetworkPolicy, Sandbox

Sandbox()  # unrestricted; no policy is sent
Sandbox(network=NetworkPolicy.block())
Sandbox(
    network=NetworkPolicy.deny_dns("github.com", "*.github.com"),
)
```

Block mode denies new network flows except the YuanRong control proxy and
published sandbox target ports used by frontend direct file I/O, reverse
tunnels, and explicit user port forwarding. Replies to allowed TCP, UDP, and
related ICMP traffic are admitted from connection state. Commands and the
frontend `/direct` filesystem path remain available; bounded RuntimeRPC chunks
remain a fallback when direct transport fails.

DNS patterns match either one exact name or descendants with a leading
`*.`; the wildcard does not match the apex. Patterns are lowercased and
trailing dots are removed. International names must be supplied as ASCII
punycode. DNS-over-HTTPS and direct connections to a known IP are not covered
by a DNS blacklist.

`block_network` and `dns_blacklist` cannot be combined. Policies cannot be
changed through this SDK after creation. The packet ACL is stateful IPv4 and
supports IPv4 fragments. The target sandboxd node must have network ACL
support enabled, and existing sandboxes must be drained before an operator
enables it.

## Configuration

| Var | Meaning |
| --- | --- |
| `YR_SERVER_ADDRESS` | Frontend gateway `host:port` for lifecycle, invoke, direct file IO. Required. |
| `YR_TOKEN` | JWT sent in `X-Auth` where required. |
| `YR_TLS` | Set `1/true/yes` to use HTTPS for frontend control routes. Default: `0`. |
| `YR_GATEWAY_ADDRESS` | Optional sandbox gateway/router `host:port` for tunnel and user port URLs. Falls back to `YR_SERVER_ADDRESS`. |
| `YR_GATEWAY_TLS` | Set `1/true/yes` to use WSS for gateway tunnel routes. Default: `0`. |
| `YR_TUNNEL_CONNECT_TIMEOUT` | Reverse tunnel WebSocket connection wait in seconds. Default: `60`. |
| `YR_TUNNEL_PROTOCOL_VERSION` | Highest reverse-tunnel protocol version to advertise. Default/cap: `2`. |
| `YR_TUNNEL_MAX_BODY_SIZE` | Per-request or response HTTP body bound advertised to the peer. HTTP bodies are streamed. Default: `512 MiB`; cap: `1 GiB`. |
| `YR_TUNNEL_MAX_WS_MESSAGE_SIZE` | Per-message application WebSocket bound advertised separately because each message is reassembled. Default: `1 MiB`; cap: `8 MiB`. Use application-level chunking for larger payloads. |
| `YR_TUNNEL_STREAM_CHUNK_BYTES` | V2 binary-frame payload bound. Default/cap: `64 KiB`; minimum: `1 KiB`. The fixed cap keeps the global 480-frame data budget below `30 MiB`. |
| `YR_TUNNEL_MAX_INFLIGHT` | Concurrent tunnel HTTP work bound. Default: `16`; cap: `1024`. |
| `YR_TUNNEL_STREAM_WINDOW_FRAMES` | Per-stream credit window and request queue bound. Default: `16`; cap: `1024`, further reduced so all negotiated HTTP windows fit the fixed outbound frame budget. |
| `YR_TUNNEL_FAST_PATH_BODY_BYTES` | Largest request/response kept on the small JSON fast path after V2 negotiation. Default: `64 KiB`; capped at the V1-safe `5 MiB` control-frame bound. |
| `YR_SANDBOX_CREATE_TIMEOUT` | Sandbox end-to-end create budget in seconds. Default: `60`; must be greater than the 30-second scheduling buffer. |

Tunnel limits are process-local, optional overrides. The SDK and rrt-runtime
advertise their values in `hello` and use the lower value, so existing AKernel
sandbox creation does not need to inject matching variables for the defaults.
HTTP body and application WebSocket message limits are negotiated separately:
large HTTP bodies stay streaming, while a WebSocket message is reassembled and
therefore uses the lower bounded limit.
Text WebSocket messages also have to fit the fixed `8 MiB` JSON control frame;
oversized/escape-expanded text is rejected on that channel without resetting
the tunnel. Large payload protocols should use application-level binary chunks.

## Build

From this directory:

```bash
PYTHON=python3 bash build.sh /tmp/openyuanrong-sandbox-dist
```

From the repository root, the build wrapper does the same:

```bash
PYTHON=python3 bash ../build.sh /tmp/openyuanrong-sandbox-dist
```

## Test

Offline transport/unit checks:

```bash
PYTHONPATH=. python3 tests/test_transport_direct.py
```

Live K8S/frontend checks need `YR_SERVER_ADDRESS`, `YR_GATEWAY_ADDRESS`, and a
valid token:

```bash
PYTHONPATH=. python3 tests/e2e_rrt_direct.py
PYTHONPATH=. python3 examples/reverse_tunnel.py
```

## Runnable examples

Only examples expected to run in ordinary SDK/K8S smoke environments are kept:

- `examples/basic_usage.py`
- `examples/command_stdin.py`
- `examples/persistent_shell.py`
- `examples/tunnel_large_response.py`
- `examples/port_forwarding.py`
- `examples/reverse_tunnel.py`
- `examples/named_sandbox.py`
- `examples/bench_cp.py`

Infra-specific demos should be documented separately instead of being shipped as
runnable SDK examples.

## Architecture

- **Control plane** — `POST /api/sandbox/v1/sandboxes`, `DELETE …/{id}`,
  `POST …/{id}/invoke` with the unified `{action, args}` model (`yr_sandbox/_transport.py`).
- **Direct data plane** — frontend/gateway `/direct/{sandbox}/...` routes for
  command invoke and binary file upload/download.
- **Reverse tunnel** — gateway `/tunnel/{sandbox}` WebSocket back to a local
  upstream (`yr_sandbox/tunnel_client.py`). Local upstream requests intentionally ignore
  host proxy environment variables.

See [`TODO.md`](TODO.md) for remaining SDK work.
