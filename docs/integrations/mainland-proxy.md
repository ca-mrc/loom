# Simulated mainland tool networking

`mainland_proxy` is an optional runtime setting, default `false`. It applies a
fixed GFWList/GreatFire rule snapshot to task tool traffic. It does not select a
mainland IP address or certify nationwide reachability. Model API calls keep
using the existing model gateway.

## User controls

Local Docker CLI:

```sh
loom run --dataset <benchmark> --agent terminus-2 --model <provider/model> \
  --backend docker --mainland-proxy
```

Hosted batch CLI:

```sh
loom eval batch create --purpose trajectory_generation \
  --agent terminus-2 --provider <provider> --model <model> \
  --task-set <task-set-id> --network-policy public-web --mainland-proxy
```

`--no-mainland-proxy` explicitly disables it; omission preserves the existing
network behavior. The same option is accepted by `loom eval run` and can be
combined with the existing execution-selection options. It is an additional
restriction on the task's permitted web access, not a new execution axis.

For a batch API request, add this to the existing request:

```json
{"trial_config": {"mainland_proxy": true}}
```

Direct trial requests use `config.mainland_proxy`. The value must be a JSON
boolean. It is persisted in the trial config and carried through fanout/rerun.
When using the CLI's complete `--request` JSON mode, put the setting inside that
file instead of combining it with creation flags.

Current supported harnesses are `terminus-2`, `oracle`, `direct-completion`,
and the legacy `litellm` alias. Other in-sandbox model clients are rejected
rather than accidentally routing provider calls through the tool filter.

## Operator configuration

The fixed snapshot directory contains:

- `gfw-rules.yaml`: Loyalsoldier Clash GFWList-derived domain provider.
- `greatfire-rules.yaml`: generated classical `DOMAIN,<host>` reject rules.
- `sources.json`: upstream release and attribution metadata.
- `greatfire-decisions.json`: dated observations and explicit decisions.

Prepare/update the snapshot between batches; do not change a running batch's
input. Prepare a new version using the repository updater (from an installed
checkout):

```sh
uv run --locked python scripts/refresh_mainland_network_profile.py \
  --output-dir /path/to/network-profiles/20261028
```

The updater fetches the latest Loyalsoldier release and existing GreatFire
observations for the documented package/Git hosts. Repeat `--url https://host`
to replace that explicit root-URL set. It never triggers new measurements or
reads trajectory URLs. It validates the snapshot before publishing a new
directory and refuses to overwrite any existing version. Point new batches at
the new directory after review; updating an input does not alter a compiled
hosted plan or an already started Docker attempt.

`gfw.txt` is the appropriate base category; `proxy.txt` and ad-blocking
`reject.txt` have different semantics.
Unknown or stale resources remain direct in this approximation and are not
labelled mainland-accessible. The current generated rules conservatively reject
fresh `intermittent` observations at exact-host granularity.

Docker workers and local CLI require:

```sh
export LOOM_MAINLAND_PROXY_IMAGE=<preloaded-mihomo-image>
export LOOM_MAINLAND_PROXY_PROFILE_DIR=/path/to/fixed-snapshot
# Official image executable; override for another preloaded image layout.
export LOOM_MAINLAND_PROXY_BINARY=/mihomo
```

The worker only advertises the capability when image/profile settings and
snapshot files exist. It never downloads a gateway image during a trial. The
image must contain mihomo and busybox `wget` for its loopback health probe.
Missing configuration, missing image, invalid rules, or a failed health check
fails setup; there is no unrestricted fallback.

Docker creates attempt-owned internal and uplink networks. The tool and
separate verifier join only internal; the mihomo gateway joins both. Worker
proxy environment overrides take priority over task proxy values. Only known
local/sidecar names are placed in `NO_PROXY`. The gateway rejects proxying to
loopback/private/link-local destinations, and its controller is not published.
Normal task sidecars use the existing controlled internal network. The gateway
explicitly disables kernel forwarding, so adding a route inside a NET_ADMIN
tool container cannot turn it into a transparent bypass. Cancellation and setup
failure clean up attempt-owned resources.

Task policy is applied before the first tool command and switched/restored for
each agent/verifier phase. The gateway always checks simulation rules before
allowing a task destination. Legacy Docker NoNetwork/Allowlist namespace rules
are retained; Allowlist gets only the proxy IP transport exception, while the
gateway still checks the actual allowed targets. Such legacy namespace policies
still require iptables in the task image.

For Nebius, set `LOOM_MAINLAND_PROXY_PROFILE_DIR` on the API/control-plane
processes that admit and compile tasks, pointing at the same immutable snapshot.
The execution pod does not need that local directory.

Nebius uses the existing forced task-egress proxy, applying the same frozen
rule data for container and guest execution. The public rule contents and source
versions are embedded in the runtime plan; the operator's local directory path
is not sent to a remote executor. The admission runtime profile must advertise
both `supports_mainland_proxy: true` and the existing task-web-egress support.
An old runtime or missing snapshot is rejected. The task must already allow
`public-web` or `web-allowlist`; this setting does not broaden `gateway-only` or
`no-network`. Setup installation keeps its existing policy; the simulation
restricts agent/verifier task traffic. Old static `service_execution` bindings
that bypass the automatic compiler are explicitly rejected with
`mainland_proxy_bound_execution_unsupported` when enabled; their disabled
behavior is preserved.

## Evidence and scope

The persisted config records the requested boolean. Docker results, trial-start
events and ATIF metadata include `tool_network_profile` with effective rule/source
versions and applied state. Native hosted plans and task-egress diagnostics
record the frozen profile and rule-denial reason. Execution-selection readback
shows the request beside the compiled profile, which is distinct from live
runtime acceptance.

Local regression covers default/false behavior, real on/off CLI execution,
separate verifier networking, missing configuration, startup cancellation,
worker capability matching, and profile persistence. Docker functional tests
use explicit local image/profile/socket variables and skip when absent. Hosted
plan/Go proxy checks run locally; no production rollout is implied by those tests.

HTTP/HTTPS downloads are the initial supported scope. The simulation does not
reproduce actual DNS poisoning, SNI resets, regional variability, throughput, or
all SSH/UDP/ECH behavior. pip/npm test cases disable arbitrary install hooks and
are not proof for every package dependency tree. GreatFire data attribution is
preserved; see <https://en.greatfire.org/data/methodology>.
