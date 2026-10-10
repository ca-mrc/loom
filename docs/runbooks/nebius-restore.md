# Independent Nebius backup restore verification

The operator restores one saved native S3 custom-format PostgreSQL backup into a
temporary database, compares selected successful Trials and their LLM/artifact
records with a source baseline, and checks their canonical S3 object references.
It uses the existing independent platform configuration and credential scopes.
It does not change the running database, create Trials, or provision nodes.

Capture the baseline while the selected records are quiescent, immediately before
the corresponding backup. Use the schema revision actually in that backup: an
older backup remains useful rollback data and must not be asserted to contain a
later migration. Generate the bounded SELECT with the deployed source:

```sh
python -m loom.nebius_restore baseline-sql \
  --trial-id SELECTED_SUCCESSFUL_TRIAL_UUID > baseline.sql
```

Repeat `--trial-id` for up to 20 existing successful Trials. Execute that SELECT
through the existing protected operator database connection with `psql -XAt
--set=ON_ERROR_STOP=1 --file=baseline.sql`, saving its JSON to a protected
`baseline.json`. Do not print a connection URL or database credentials. The
baseline contains the schema revision and each Trial's ID, state, aggregate
reward, LLM call count, input/output tokens, and artifact count. It requires at
least one LLM call and artifact per selected Trial. Then run the existing backup
Job and record its native S3 key and version. This is a consistency check for a
quiescent snapshot, not a new source fingerprint or approval manifest.

Plan first using the reviewed render directory for this independent platform:

```sh
python scripts/ops/verify_nebius_restore.py \
  --render-dir /protected/platform/render \
  --kubeconfig /protected/platform/kubeconfig \
  --expected-cluster-id mk8scluster-EXISTING_INTEGRATION_CLUSTER \
  --backup-key loom-nebius-platform/YYYY/MM/DD/HHMMSS-CHECKSUM.dump \
  --backup-version-id NATIVE_S3_VERSION \
  --baseline /protected/baseline.json \
  --evidence-dir /protected/restore-plan
```

The default performs target/Secret-key preflight and writes the two resource
specifications without creating them. Run the same command with `--apply` and a
fresh evidence directory to perform the verification. The Service image in the
render must include `loom.nebius_restore`; the PostgreSQL image is the existing
pinned `backup_image`. No separate restore image or controller is installed.

Only a uniquely named ConfigMap and a Job are created in `loom-nebius-platform`.
The Job selects both `loom.nebius/platform=integration` and
`loom.nebius/node-role=system`, tolerates the existing integration taint, and has
no public Service. Its three sequential phases are:

1. Download only the requested backup from the configured backup bucket, using
   the existing backup access-key pair. Check native version, recorded SHA-256
   metadata and actual size at this download boundary. Reject backups over 1 GiB
   before GET; `--max-backup-bytes` permits a bounded override up to 4 GiB.
2. Initialize an empty PostgreSQL 16 database and restore with
   `pg_restore --no-owner --no-privileges --exit-on-error --jobs=1`. PostgreSQL
   listens only on an owner-only Unix socket. Export a bounded selected-record
   snapshot, then stop PostgreSQL before the final verifier starts.
3. Compare the restored revision and records with the baseline. With the existing
   canonical storage identity, HEAD referenced artifact and trajectory objects
   only in the configured canonical buckets, checking stored sizes and native
   versions. Emit counts and a safe completion summary, never database rows.

`--no-owner --no-privileges` intentionally omits source role ownership and ACLs,
including references to `loom_service`, `loom_control_plane`, `loom_gateway` and
`loom_actuator`. The schema and data are restored, without copying credentials or
requiring production roles in the disposable database. Actual replacement of a
production database would separately run the supported role/bootstrap procedure;
this command does not perform that operation.

The Job requests 100m CPU, 256 MiB memory and 128 MiB ephemeral storage, capped at
500m CPU, 1 GiB memory and 8 GiB scratch space. PostgreSQL uses 32 MiB shared
buffers and one restore worker. It has a 30-minute deadline and no automatic
retry. Scratch storage is an EmptyDir; no production PVC, DB URL, DB Secret,
service-account token or TCP PostgreSQL listener is provided. Large backups can
exceed the scratch cap even when their compressed size fits; a failure remains
visible instead of provisioning more capacity automatically.

`restore.json` and `restore.yaml` are protected local evidence. Success is saved
before deleting only the uniquely created Job and ConfigMap. Failures retain the
Job and scratch Pod for diagnosis. Bounded phase/exit/error codes are collected;
raw `pg_restore` output stays in the private `/restore/pg_restore.log` and must
not be copied to ordinary CI logs or shared artifacts. After diagnosis, remove
only the Job and ConfigMap named in that evidence. A successful check proves the
selected database records can be restored and current canonical references can
be read; it does not simulate loss/restoration of the entire object store or
replace a complete cross-environment disaster recovery exercise.

The disposable integration qualification in
`tests/integration/test_nebius_restore.py` also exercises application recovery:
it stops the source database, restores its ACL-bearing dump into a separate
database, and recreates restricted application roles through the supported
`bootstrap_database` procedure with fresh test credentials. The actual Service
startup rejects an incompatible encryption key. With the original test key,
password login, team isolation and authenticated artifact downloads work; a
missing object fails its own download while another team's saved result remains
readable. This uses the Service's `api_only` mode and ASGI HTTP transport with
real PostgreSQL and MinIO. It does not exercise hosted ingress, the browser,
worker startup or replacement of the running deployment, and does not extend
the selected-record operator above into a full application restore command.
