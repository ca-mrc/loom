# Prepare the reviewed Poetry and Jupyter task revisions

The September 24 private packages need task-owned environment declarations:

| Task | Correction |
| --- | --- |
| `rstq1k__rts_task_01097437652020bc2a937308` (Poetry) | Declare the image's `/usr/local/bin/python3.9` as both a workspace and mutable-path reference, so real virtualenv links survive independent verifier handoff. |
| `rstq1k__rts_task_030c19830d06ef8d9b27ac0f` (Jupyter) | Transfer the user registration root `/root/.local/share/jupyter` and both system roots `/usr/local/share/jupyter`, `/usr/share/jupyter`. Create empty roots in the image so snapshot validation can run before the agent registers kernels. |

Prepare each new revision from its downloaded immutable source:

```sh
uv run --no-sync python scripts/ops/repair_quality_task_declarations.py \
  --source /path/to/original-task --output /path/to/new-task-revision
```

The command requires the exact reviewed source checksum and a new output
directory. It changes only `task.toml` and, for Jupyter, the Dockerfile's empty
registration roots. Instructions, tests, solutions, model settings and timeouts
remain unchanged. It prints the old/new checksums and changed files; it neither
uploads a package nor runs a model. Private task content stays outside Git.

Publish each output as a new immutable task revision through the normal backend
CLI under the owning team, build/preflight its image, and verify the revision
selected by a subsequent acceptance run. Do not overwrite old task sources or
historical results. Existing timeout/model-failure attempts remain valid results;
these declaration fixes do not retroactively make them successful.

`tests/integration/test_quality_declaration_handoff.py` verifies actual Python
3.9 virtualenv links and all three Jupyter registration roots across fresh
sandbox boundaries, with no model calls. This boundary check does not substitute
for executing the newly published private tasks with the original model config.
