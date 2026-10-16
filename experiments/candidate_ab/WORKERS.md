# Download-phase build-worker comparison

Manual-only follow-up on the existing isolated experiment branch; no PR, protected
build identity, production registry publication, signing or deployment.

Application SHA remains `c60ea0083a924c5708b18376fc5df32232a746d9`. Native producer
run `38966009027` is pinned to the preceding tool commit and regenerates the same
seven image artifacts after the previous task's temporary-artifact cleanup. Each
new consumer downloads the same four artifacts from that producer through the
official native action and its checksum. The other three producer artifacts are
not downloaded in this experiment and are cleaned up after all consumers finish.

Six balanced blocks each run the same two scenarios (partial-hit, late full
fallback) at three download-phase build-worker limits. Each scenario sees all
six permutations of 1/2/3 exactly once. Scenario order alternates by block.
Within a block all cases share a VM and pinned scanner/DB seed, each with a fresh
BuildKit and fresh analysis cache. Different block jobs can have different VM
load/DB refreshes; compare arms paired by block, retain DB metadata and runner
identity. `max-parallel: 2` bounds experiment VM demand.

`blocks` is JSON selecting from `[1,2,3,4,5,6]`. Start `[1]` first to verify native
resource sampling and the real phase boundary, then run remaining blocks on the
same workflow commit. No samples are discarded to choose a faster result.

`download_buildctl.py` gates the three early-export commands using OS file locks.
During native transfer only the treatment's 1/2/3 command slots are available.
At finish entry a native-download completion marker releases all three slots;
already-running builds continue. Thus the intervention limits only the download
phase, not the entire early batch. Real candidate helper/build arguments remain
unchanged. The known-miss worker is joined before the existing mandatory OCI
validation and late fallback pool, which remains three workers. Export failures
stay fatal. Gate start/end records prove actual admission/phase and queue wait.

Partial-hit reuses four and rebuilds web/actuator/Harbor. Full-fallback passes the
same native download/checksum, then rotates component filenames to trigger the
existing identity rejection, so all seven are rebuilt at the exact source SHA.
This is late component mismatch, not a measured network/digest failure.

Each case preserves all metadata/runtime/platform/ref checks, 21 Trivy scans,
7 no-exceptions policy validations and 7 local-registry publication digest checks.
Producer additionally retains its Docker/OCI format controls and Harbor offline
probes. The loopback registry is not a surrogate for Nebius WAN/deployment cost.

Read-only telemetry samples procfs/cgroupfs once per second. It includes VM CPU,
steal/iowait, memory, CPU/I/O/memory PSI, disk and default-route network counters,
plus BuildKit CPU use/throttling, memory peak/events and I/O. Docker inspect runs
once per case. Sampling errors are retained and fail the measurement rather than
silently substituting zero; normal collector completion is joined before builder
cleanup. Collector overhead is common to all arms. VM signals cannot prove what
the shared physical OLDLAB host was doing.

Primary elapsed metric is the complete native case-step envelope, with internal
high-resolution total including collector stop/cleanup separately retained.
Report download window, early build admission/durations, post-download join,
late preparation, scan/publication, cleanup and resource summaries by phase.
Show every sample, paired block differences, medians, empirical p90/p95 and max.
Six samples per treatment do not establish a production p95 or confidence bound.
Queue/setup are measured per block and kept separate from algorithm elapsed;
do not invent separate allocations for each of its six cases.

End-to-end deployment figures, if shown, use the previous same-SHA deployed
production baseline and preserve its real gates, serialization, WAN upload,
signing and rollout costs. They are explicit models, not a new deployment.
