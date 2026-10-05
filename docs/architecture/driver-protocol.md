# Driver Protocol

The `Driver` Protocol is the sandbox-lifecycle contract every backend
implements. Lives at `src/loom/driver/base.py`.

## Contract

```python
from typing import Protocol, runtime_checkable, Literal
from pathlib import Path, PurePosixPath
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass

from loom.models.capabilities import Capabilities
from loom.models.exec import ExecResult
from loom.models.healthcheck import HealthcheckSpec
from loom.models.networking import NetworkPolicy

MAX_EXEC_STREAM_BYTES: int = 10 * 1024 * 1024  # 10 MB

@dataclass
class StartOptions:
    force_build: bool = False
    pull: bool = True

@dataclass
class ExecHandle:
    pid: int
    stdout: AsyncIterator[bytes]
    stderr: AsyncIterator[bytes]
    _wait: Callable[[], Awaitable[int]]
    _kill: Callable[[], Awaitable[None]]
    async def wait(self) -> int: ...
    async def kill(self) -> None: ...

@runtime_checkable
class Driver(Protocol):
    image: str
    workspace: PurePosixPath
    capabilities: Capabilities
    os: Literal["linux", "windows"]
    network_policy_baseline: NetworkPolicy

    async def start(self, *, options: StartOptions | None = None) -> None: ...
    async def stop(self, *, delete: bool = True) -> None: ...

    async def exec(
        self, cmd: str, *,
        user: str | int | None = None,
        cwd: PurePosixPath | None = None,
        env: Mapping[str, str] | None = None,
        timeout_sec: float | None = None,
    ) -> ExecResult: ...

    async def exec_streaming(
        self, argv: list[str], *,
        env_vars: dict[str, str],
        cwd: PurePosixPath,
        user: str | int | None = None,
    ) -> ExecHandle: ...

    async def upload(self, src: Path, dst: PurePosixPath) -> None: ...
    async def download(self, src: PurePosixPath, dst: Path) -> None: ...

    async def set_network_policy(self, policy: NetworkPolicy) -> None: ...
    async def run_healthcheck(self, hc: HealthcheckSpec | None = None) -> None: ...
```

## Lifecycle invariants

- `start()` once per instance. Second call raises
  `DriverAlreadyStartedError`.
- `stop()` is idempotent. Calling `stop()` before `start()` is a
  no-op, not an error.
- `exec` / `upload` / `download` / `set_network_policy` /
  `run_healthcheck` require `state == "running"`. Otherwise raise
  `DriverNotStartedError`.
- `stop(delete=False)` is for archive flows (Docker doesn't fully
  support it; cloud backends may). Default is `delete=True`.

State machine: `constructed → running → stopped`. No re-start.

## Capability axes

`Capabilities` describes both the sandbox backend contract and the worker host
where that backend runs. The scheduler currently matches `os`, `cpu_arch`,
`gpu_vendor`, and `network_policies` from `trials.requires_caps`. `cpu_arch`
uses worker values `x86_64` or `arm64`; task requirements may also use `any`
only for images and verifiers proven credible on both architectures. Missing
legacy trial requirements are treated as `x86_64` so ARM64 worker pools do not
claim historical x86-specific work.

## Output buffering

- `exec()` is buffered and capped at `MAX_EXEC_STREAM_BYTES = 10 MB`.
  Larger outputs truncate; `ExecResult.truncated == True`. Docker applies
  each stream's cap while reading, then drains the remaining bytes without
  retaining them so the command can finish and report its actual exit code.
- `exec_streaming()` has no total-output cap. Callers drain `stdout` + `stderr`
  in parallel. Docker retains at most 1 MiB of queued chunks per stream and
  applies socket backpressure when a consumer falls behind; it also bounds
  pending event-loop notifications. Normal `wait()` preserves unread output
  through transport EOF before returning the exit code; concurrent and repeated
  waits are supported. Stop, kill and wait cancellation close the transport,
  release blocked producers and signal EOF even if the reader has not started.
  Consumers may drain the already queued prefix after these explicit aborts.
- File-tail and HTTP-poll launcher capture drain and discard unused console
  stdout while reading their event source. The worker retains responsibility
  for stderr diagnostics; each capture helper joins its own background tasks
  on completion or cancellation.
- Callers that cancel a long-running `exec_streaming()` operation before
  `wait()` resolves must call `ExecHandle.kill()` best-effort and clean
  up any stream-drain tasks. `SubprocessAgent` does this when an agent
  step timeout cancels the adapter run.

## NetworkPolicy

`loom.models.networking` exports three kinds:

```python
Public()                                                          # no restriction
NoNetwork()                                                       # block all egress
Allowlist(domains=("api.example.com",), cidrs=("10.0.0.0/8",))   # allow these only
```

Domain resolution is backend-specific:
- **DockerDriver** resolves via `getent ahosts` (IPv4-only), pins
  IPs into `/etc/hosts`, sets iptables default DROP with explicit
  ACCEPT rules per IP/CIDR. Requires `cap_add=["NET_ADMIN"]`.
- **FakeDriver** records the policy on `self.network_policy_baseline`;
  no enforcement.

Unresolvable domain → `DriverError` (no silent drop).

## Included implementations

### `loom.driver.docker.DockerDriver`

Local Docker. The reference impl — most other drivers follow its
shape.

```python
from loom.driver.docker import DockerDriver
drv = DockerDriver(image="python:3.12-slim", workspace=PurePosixPath("/workspace"))
await drv.start()
r = await drv.exec("ls /workspace")
await drv.stop()
```

Specifics:
- Containers run with `cap_add=["NET_ADMIN"]` for iptables policy
- `exec_streaming()` uses `docker exec` with TTY-less pipes
- Healthcheck loop polls `cmd` with `start_period_sec` grace +
  `retries` consecutive failures before raising

### `loom.driver.fake.FakeDriver`

Test harness — every `exec` returns success with empty bytes.
Useful for wiring smoke (the trial completes successfully without
spending compute), but no real solver work happens. The
`--backend fake` CLI choice uses this.

### Modal

`src/loom_drivers/modal/`. Bridges Modal's sync SDK via
`asyncio.to_thread` (same pattern DockerDriver uses for docker-py).
Snapshot reuse via per-process `modal.Image.from_id()` cache keeps
cold starts down. GPU passthrough uses `Capabilities.gpu_types` plus a
`--gpu <TYPE>` CLI flag on
`loom run`. Cost telemetry routes through the same
`cloud_compute_records` table, tagged `cloud_provider="modal"`;
`/api/v1/usage` exposes `modal_compute_seconds` +
`modal_cost_usd` alongside provider-neutral cloud totals. Auth via
`MODAL_TOKEN_ID` + `MODAL_TOKEN_SECRET`. Live
integration test opt-in via `LOOM_RUN_MODAL_INTEGRATION=1`.

## Adding a new driver

1. Pick a Driver as reference. For SDK-bridged sync-style APIs that need a
   threadpool, copy `loom_drivers/modal/`
   (specifically its `client.py` `asyncio.to_thread` bridge pattern).
2. Implement every Protocol method. Run
   `pytest tests/contract/test_driver_contract.py` — the parametrized
   contract suite exercises Protocol conformance against every
   registered Driver impl.
3. If the cloud backend needs cost telemetry, reuse
   `loom.cost.cloud_records.CloudComputeRecord` and `persist_record`.
   The `cloud_compute_records` table is shared across providers via the
   `cloud_provider` column.
4. Wire into `src/loom_cli/run_cmd.py::_driver_factory` for
   `--backend <name>` support. Add `<name>` to the `--backend`
   `choices=` tuple in `src/loom_cli/__main__.py`.
5. Add unit tests with an `AsyncMock` SDK seam; a live integration
   test should be opt-in via an env var (e.g.
   `LOOM_RUN_<NAME>_INTEGRATION=1`).

## Common pitfalls

- **`stop()` should not raise**. Even if the cloud delete fails, log
  + continue. Use `asyncio.wait_for(delete, timeout=N+5)` so a stuck
  delete doesn't hang `stop()`. Keep retry authority durable when a
  provider deletion fails.
- **GIL + signal handlers**: cloud drivers that install SIGINT
  handlers should keep handler bodies minimal. `asyncio.run` inside
  a signal handler is technically not async-signal-safe; it works
  for the Ctrl-C exit path but multi-threaded workloads should
  prefer the `atexit` path.

## See also

- [overview.md](overview.md)
- `src/loom/driver/base.py` — the Protocol source
- `tests/contract/test_driver_contract.py` — the parametrized
  conformance suite every Driver should pass
