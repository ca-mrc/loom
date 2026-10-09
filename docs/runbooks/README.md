# Runbooks

Repeatable procedures for the supported Nebius platform and local development.
Begin with the [operator runbook](operator-runbook.md). Use the architecture
[platform contract](../architecture/nebius-primary-platform.md) for behavior and
[CI documentation](../contributing/ci.md) for repository validation.

## Platform and delivery

- [Infrastructure](nebius-infrastructure.md): Terraform, identities, networking and resource convergence.
- [Platform operations](nebius-platform.md): service rendering, capacity and deployment diagnostics.
- [Candidate publication](nebius-candidate.md): immutable images and candidate records.
- [Deployment](nebius-deployment.md): target binding, backup, migrations and readiness.
- [Staging credentials](nebius-staging-credentials.md): scoped identities and PostgreSQL trust.
- [Staging release validation](staging-launch.md): candidate-bound promotion evidence.

## Recovery and execution

- [Restore verification](nebius-restore.md): isolated backup restoration.
- [Historical lineage conversion](nebius-lineage-conversion.md): qualified conversion of divergent Nebius revisions.
- [Accounting repair](nebius-accounting-repair.md): recover a previously committed Terminus export.
- [Verifier archive recovery](nebius-verifier-archive-recovery.md): request one audited retry for the legacy failed-verifier projection defect.
- [Cold-start diagnosis](nebius-cold-start.md): capacity and scale-from-zero evidence.
- [Execution security](nebius-execution-security.md): bounded isolation validation.
- [Trial resource accounting](trial-resource-accounting.md): usage durability and capacity calibration.
- [Deadline canary](isolated-deadline-canary.md): isolated timeout fixture and its acceptance boundary.

## Workload preparation

- [Terminus-2](nebius-terminus2.md): prepare and validate native Terminal-Bench execution.
- [Harbor runtime versions](harbor-runtime-versions.md): select and register a published runtime.
- [Harbor90 x86 sources](nebius-harbor90-migration.md): preserve benchmark identity while publishing x86 inputs.
- [TerminalGen corpus publication](terminalgen-corpus-publication.md): publication and read contracts.

## Personal and local development

- [Personal application workflow](personal-development.md): build your frontend/API, run a smoke task, inspect results and manage your application.
- [Local workflow](local-dev-workflow.md): disposable Compose stack, checks and reset.

Create a runbook for a repeatable procedure with distinct safety or recovery
requirements. Keep implementation plans outside the repository. Retain only
necessary schema/decision context in [history](../historical/README.md).
