# Native Actions same-SHA candidate A/B

Manual-only experiment on `codex/candidate-artifact-transfer-20261016`; never a
merge gate, admitted candidate, signer, production registry writer or rollout.
Actual read-only production artifact metadata lookup is timed per consumer arm.
Its real result is retained separately from explicitly controlled cache fixtures;
manual experiment runs are never qualified merge-group evidence. The metadata
token is removed before launching an unsigned worker.

The independent application checkout is pinned by `source_sha`; workflow/tool
SHA, app SHA, runner and budgets are recorded separately.

One producer builds seven exact-SHA OCI files and preserves all 21 scans,
7 no-exceptions policy checks and 7 loopback publication digest checks. A warm
Docker/OCI export and scan-format control measures incremental premerge work;
Harbor keeps its Docker export and both offline behavioral probes in each arm.
The extra experimental control work is not production overhead. Seven raw native
upload-artifact v7 files are the cross-run inputs. Export, scan and upload costs
remain separate.

One OLDLAB native consumer job runs six cases with fresh BuildKit containers,
no imported build cache, a shared pinned DB seed but fresh analysis caches,
4 CPUs and 3 workers. Both helpers use the same source, mirror configuration,
validation and native download-artifact v8. One VM is intentional: it controls
runner differences; queue/setup costs are shared observations, not six separately
measured allocations. Each case retains its own cleanup duration.

- All hit: download all seven, inspect and reuse all seven.
- Partial hit: download service/control_plane/gateway/execution_runtime (the four
  components in the latest observed #2543 image selection), rebuild the other
  three. The producer uploads seven to serve the full experiment; charge all
  seven actually uploaded when accounting experiment cost. A four-only production
  upload cost is a subset estimate, not another producer job measurement.
- Full fallback: download those same four through the native action and its
  digest check, then cyclically mismatch only their component filenames as a
  declared cache-boundary fixture. The four existing component validations reject
  them; all seven images must be rebuilt. This exercises late identity failure,
  not a native network failure or checksum failure. Native failures still disable
  the whole batch, and early new-build failures remain fatal.

Old waits for download before image preparation. New exports known misses during
native transfer, joins the actual worker result, validates mandatory local fresh
outputs, then schedules late fallbacks within the same worker bound. Every
consumer case keeps 21 scans, 7 no-exceptions policy checks and 7 publication digest
comparisons. Native GitHub identity is never overwritten and the protected build
entrypoint is not called.

The builder mounts only existing `source/config/ci-buildkit.toml` read-only and
explicitly uses it. Bootstrap configures only this disposable VM's Docker mirror
using the existing repository helper; it never changes an OLDLAB host daemon.
The experiment uses a loopback registry for publication validation. Real Nebius
WAN publication, gates, serialization, runner queue and deployment remain explicit
external costs in the end-to-end model; no deployment result is simulated.

Metrics and full scan/policy/SBOM reports are retained as a small compressed native
artifact. Large raw payloads should be deleted only after consumers finish and
local metrics/evidence have been retained. The final cleanup removes only this
job's temporary root and uniquely named builders/registries.
