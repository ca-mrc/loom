# loom-benchmark-terminal-bench-2

Harbor-native adapter for Terminal-Bench 2.1 revision 6.

The package keeps the `terminal-bench-2` entry-point key, while its physical
adapter profile is `terminal-bench-2@tb2.1-r6`. It converts exactly the 89
tasks admitted by the checked-in lock for Harbor Hub dataset
`terminal-bench/terminal-bench-2-1@6`.

## Source authority

Harbor Hub metadata version
`sha256:7d7bdc1cbedad549fc1140404bd4dc45e5fd0ea7c4186773687d177ad3a0699a`
and its 89 immutable package digests are the execution authority. Conversion
fails closed unless `harbor-materialization.json` and the independently staged
source-reference manifest agree with the lock.

The source-reference snapshot is
`dde3cd95b80ff25af5abd99a80b6513a018ad3b4`. Its sole reviewed package digest
divergence, `terminal-bench/sanitize-git-repo`, is provenance only; it never
changes the Hub execution package or introduces a fallback source.

## Native conversion

The adapter accepts only native directories in this shape:

```text
tasks/<task>/
  task.toml
  instruction.md
  environment/
  tests/test.sh
  solution/solve.sh
```

It preserves the source `task.toml` byte-for-byte as `upstream-task.toml` and
writes a runnable Loom schema-1 `task.toml`, changing the Loom task id to
`terminal-bench-2@tb2.1-r6/<task>`. Supported timeouts, image/build settings,
environment values, task metadata, and artifact paths are normalized into the
runnable config. Native CPU, memory, storage, GPU, internet, architecture, and
build limits are projected into the runnable contract and attested in task
provenance; unsupported environment semantics fail publication instead of
falling back to a generic image. `upstream-task.toml` still retains every
source field byte-for-byte. Instructions, environment assets, tests, and
solution files retain their original bytes and modes.

Oracle eligibility is derived only from a non-empty, non-symlink native
`solution/solve.sh`; it is not inferred from a task-id prefix.

## Verifier and reports

`verifier/run.sh` runs the native `tests/test.sh`, copies its test tree to the
native `/tests` location, and reads `/logs/verifier/reward.txt`. A finite
numeric reward, including `0`, is a valid verifier result. Missing, empty,
malformed, non-finite, or timed-out reward evidence is a platform/verifier
failure and is never converted to zero. CTRF and verifier-log paths are kept
in the structured verifier result. The runnable Loom step preserves only the
source task's authored artifact patterns; it does not append `logs/verifier/**`.
The native verifier runtime independently collects the structured verifier result
and optional CTRF, including when a source task declares `artifacts = []`.

TB2 report conversion normalizes both `terminal-bench-2/<task>` and
`terminal-bench-2@tb2.1-r6/<task>` to the short task ID. Physical revision-6
trial reports also record the profile, Hub package digest and metadata version,
source-reference snapshot/divergence, Loom bundle checksum, verifier identity,
and separately supplied runtime provenance.

## Publication and activation

Publishing emits manifest schema 4. The manifest and every task record the
Hub package digest, Hub metadata version, source-reference snapshot (including
the sole reviewed divergence), verifier identity, image provenance, and the
private workspace staging policy. Every task additionally records the native
configured `verifier/run.sh` path and SHA-256. Register persists that evidence
to the physical profile and task rows but never changes the public alias;
TB2.1 rows stay `pending` and cannot be submitted through either the public
alias or a direct physical selector.

The normal-agent workspace staging path excludes `solution/`, `tests/`,
`verifier/`, and `upstream-task.toml`; the worker fails closed if a rev-6 task
lacks that exact persisted policy. The verifier runs in a new driver/container:
only public agent-workspace files cross from the completed agent driver, while
the fresh verifier driver receives the private subset. To move
`terminal-bench-2`, an operator must produce a complete 89-bundle audit result
and then call:

```bash
loom datasets activate terminal-bench-2 \
  --profile terminal-bench-2@tb2.1-r6 \
  --audit-json "$PWD/tb21-audit.json" \
  --minio-endpoint "$LOOM_MINIO_ENDPOINT" \
  --minio-access-key "$LOOM_MINIO_ACCESS_KEY" \
  --minio-secret-key "$LOOM_MINIO_SECRET_KEY"
```

The JSON carries the prior audit's snapshot identity, not a trusted clean
assertion. Activation locks the current catalog rows, downloads and audits all
89 current bundles again, marks the profile `runnable`, records the resulting
immutable snapshot on the physical profile, and upserts the alias in that
transaction only when the snapshots match and the fresh audit is clean. The
snapshot excludes mutable activation metadata, so a fresh audit after
activation has the same identity. Before execution, a worker rehashes its
materialized bundle against `Task.checksum`; modifying an object after audit
therefore fails closed rather than executing unaudited bytes. Activation
rejects stale/forged evidence, provenance/config/checksum drift, incompatible
task images, missing or non-executable verifier assets, and any missing
private-workspace isolation.

Native execution must use an audited physical task ID, such as
`terminal-bench-2@tb2.1-r6/<task-name>`. A failed rev-6 activation or canary
disables TB2 submission; it cannot fall back to TB2.0 or reduce the 89-task audit.

## License

Apache-2.0, following the Terminal-Bench 2.1 source repository.
