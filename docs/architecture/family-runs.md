# Family runs

The retained family-run implementation serializes related tasks and carries shared
state between them. Its configuration is opt-in per catalog entry or batch.
The current Nebius-native service scheduler selects only tasks without a
`family_key`, so this page describes the retained contract, not supported hosted
family execution. Ordinary batches continue to fan out independent trials.

## Configuration

Catalog `family_run_defaults` and `trial_config.family_run` are merged when the
batch is accepted. The per-batch value wins for each field. Enabling the mode
requires all six plugin roles:

- `family_key_extractor`
- `sequencer`
- `advance_predicate`
- `adapter`
- `failure_policy`
- `state_backend`

The resolved immutable specification is stored on `batches.family_run_spec`.
`mount_path` defaults to `/root/.skills`.

## Retained execution contract

1. The submit path groups tasks with the configured key extractor and writes a
   deterministic task sequence for each family.
2. The retained family scheduler claims only the task at a family's
   `current_index`. Separate families remain independently claimable on that path.
3. Before a trial starts, the Worker downloads the family's state through the
   configured backend and bind-mounts it read/write at `mount_path`.
4. Trial finalization applies the advance predicate. A family either advances,
   retries the position, skips it, or aborts.
5. When state evolution is required, the family row becomes `adapting`. The
   optional `loom-family-orchestrator` process claims one adapting family with
   `FOR UPDATE SKIP LOCKED`, calls the adapter, and returns the row to `pending`
   or a terminal family state.

Adapter calls are bounded by `family_adapter_call_timeout_sec`. A failed call is
handled by the configured policy as `retry_with_backoff`, `skip_and_advance`,
or `abort_family`; failures do not terminate the orchestrator process.

## Built-in plugins

| Role | Built-ins |
|---|---|
| Key extractor | `instance_id_prefix` |
| Sequencer | `alphabetical`, `ranking_file`, `submitted_order` |
| Advance predicate | `always_on_terminal`, `success_or_retry_exhausted` |
| Adapter | `noop`, `skill_patcher_llm` |
| Failure policy | `stall_family`, `skip_and_advance`, `abort_family` |
| State backend | `s3_artifacts` |

Plugins are resolved through the `loom.family.*` entry-point groups declared in
`pyproject.toml`. Unknown groups, missing names, or incompatible plugin objects
fail validation before the batch is accepted.

## Shared skill state

`skill_patcher_llm` initializes a family skill tree, asks the configured model
to evolve it from the completed trial evidence, and uploads the resulting tree
through `s3_artifacts`. The next family trial receives the updated tree at the
configured mount path. Model calls use the Loom gateway, so usage and failures
follow normal provider accounting.

## Deployment boundary

The database state, plugins, family scheduler gate, and Worker mount path remain
in the repository. They do not establish family support in the current
[native service execution](nebius-service-execution.md) path. Starting the separate
`loom-family-orchestrator` process does not make family tasks eligible for that
scheduler.

On the retained path, state evolution requires `loom-family-orchestrator`; if it
is not running, families that enter `adapting` remain there and later tasks in
that family are not claimable.

Relevant settings are defined by
`src/loom_family_orchestrator/settings.py`. Health is observable through the
family state rows and orchestrator logs; ordinary non-family batches are not
affected by orchestrator availability.
