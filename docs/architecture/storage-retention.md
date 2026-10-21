# Storage Retention

Loom can apply server-side lifecycle expiration to S3-compatible object stores
with `loom cluster bootstrap-storage-lifecycle`. The command reads a strict
TOML policy, renders deterministic bucket rules, and applies them through the
configured S3 client.

## Supported live path

The live apply path supports `minio`, `s3`, `r2`, `b2`, and `wasabi`. The
policy backend must match `LOOM_SVC_STORAGE_BACKEND`. Endpoint, region, and
credentials come from the service storage settings or the documented MinIO
root-credential fallback.

Non-S3 backends, including `gcs`, are rejected when loading the policy.
The unused GCS renderer and unimplemented apply API have been removed.
Unknown fields, misspelled strategies and non-integer durations are rejected
before rendering or applying a policy. The previously ignored `rule_id` field
is rejected; rendered rule IDs remain `loom-<bucket>`.

## Policy file

Start from `config/storage-lifecycle.example.toml`:

```toml
backend = "minio"

[[retention]]
bucket = "trajectories"
strategy = "expire_after_days"
days = 30

[[retention]]
bucket = "artifacts"
strategy = "expire_after_days"
days = 180

[[retention]]
bucket = "atif"
strategy = "keep_forever"
```

Current strategies are:

| Strategy | S3-compatible effect |
| --- | --- |
| `expire_after_days` | Emits one enabled expiration rule for the bucket |
| `keep_forever` | Emits no new expiration rule; existing external lifecycle policies still apply |
| `cleanup_incomplete_uploads_after_hours` | Parses but is not emitted by the S3-compatible renderer |

The multipart-cleanup strategy is omitted because MinIO drops that action on
round-trip, which would create permanent drift. Do not rely on it to clean
incomplete uploads.

Multiple different strategies may target a bucket, but duplicate instances of
the same strategy and conflicting `keep_forever`/expiration policies are rejected. Expiration actions for one bucket are merged
into a stable `loom-<bucket>` rule. Reapplying the same policy is idempotent.
Buckets whose rendered policy is empty are skipped; Loom does not delete
unrelated lifecycle rules from them.

## Apply and verify

Inspect the S3-compatible render:

```bash
uv run --no-sync loom cluster bootstrap-storage-lifecycle \
  --config /secure/storage-lifecycle.toml \
  --dry-run
```

Apply it:

```bash
uv run --no-sync loom cluster bootstrap-storage-lifecycle \
  --config /secure/storage-lifecycle.toml \
  --endpoint http://127.0.0.1:9000
```

Check live rules against the same policy:

```bash
uv run --no-sync loom cluster doctor \
  --storage-lifecycle-config /secure/storage-lifecycle.toml \
  --storage-lifecycle-endpoint http://127.0.0.1:9000
```

Storage lifecycle expiration is separate from the retired
[staging data lifecycle](../historical/staging-data-lifecycle.md). Its retained
exact-object records explain historical data; they are not a current deletion procedure. See the
[operator runbook](../runbooks/operator-runbook.md#storage-retention)
for credential and port-forward handling.
