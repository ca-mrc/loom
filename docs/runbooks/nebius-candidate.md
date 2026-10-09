# Nebius integration candidates

This independent publication lane advances #1798. It builds the six platform
images from one `dev` commit, scans them, pushes directly to Nebius
Registry and records the commit and six immutable image references. It also
produces the existing service-execution runtime profile required by Control Plane.
It does not deploy, change `dev`/`main` authority, or prove live acceptance.

On `dev`, changes to publication authority (the candidate publisher, workflow,
registry authentication and shared Trivy report validator) retain full validation.
Ordinary Nebius deployment/render/restore files use their specific test consumers;
runtime changes, explicit CI labels and unknown paths can select additional work.
See [Nebius CI integration](../contributing/ci.md) for the combined platform contract and
optional integration coverage diagnostic.

## Bootstrap

Publication runs on GitHub-hosted Ubuntu 24.04 AMD64. CI and publication require
no Nebius runner node groups, ARC controller, runner registration credentials or
custom runner image. The workflow installs version-pinned Skopeo from Ubuntu and
starts the pinned BuildKit container on the disposable hosted machine.
Its client is copied from that same immutable image. The daemon's container port
is published only on host loopback; no host network, host workspace mount, or
Docker socket is passed into the builder. Workspaces and builder data disappear
with the job, and an always-run cleanup retains bounded, sanitized daemon diagnostics before removal.
The standard privileged BuildKit container runs only on that disposable hosted
machine, with no host socket or workspace mount. The rootless daemon exited before
listening in the first GitHub-hosted run; this supported container mode removes
its dependency on host user namespaces.
Failed publication commands use the same bounded diagnostic sanitizer.

Create environment `nebius-integration`, allowing only `dev`. An existing
isolated-branch Environment must have its branch policy deliberately updated
before `dev` publication can run; merging source does not alter that policy.
Before deploying to an existing Nebius database, follow the
[database lineage boundary](../architecture/nebius-primary-platform.md#database-lineage-when-moving-from-the-isolated-branch).
After the environment and trust configuration are verified, set repository variable
`NEBIUS_RELEASE_ENABLED=true` to enable publication on `dev` pushes.
Configure variables `NEBIUS_REGISTRY_PREFIX` (a Nebius registry repository
prefix), `NEBIUS_IMAGE_ADMISSION_SIGNING_KEY_ID` and
`NEBIUS_IMAGE_ADMISSION_TRUSTED_KEYRING_JSON`. Configure secrets
`NEBIUS_REGISTRY_SERVICE_ACCOUNT_JSON` (a registry-writer service account's
authorized-key credentials JSON, with `subject-credentials` using RS256) and
`NEBIUS_IMAGE_ADMISSION_SIGNING_KEY` (established Ed25519 PEM private key).
The independently provisioned public keyring must also be supplied to deployment
and Control Plane trust configuration. Publication never generates a new trust
root or installs the keyring. Grant the registry identity only its intended
registry write scope. The locked SDK exchanges its authorized key for a fresh
access token at publication time and before each image push. The shared temporary
Docker/Skopeo auth file uses `iam` and mode 0600; credentials and auth
files live outside the build context and are removed at job exit. A copied access
token is not a supported bootstrap credential. Rotate/revoke the authorized key
through IAM as needed, without editing pipeline code. See the
[Nebius service-account authentication contract](https://docs.nebius.com/container-registry/authentication).

Runtime readiness is explicit and remains disabled by default. The publication
workflow accepts the persistent variables `NEBIUS_TASK_WEB_EGRESS_READY`,
`NEBIUS_SERVICE_LIFECYCLE_READY` and `NEBIUS_TASK_IDENTITY_READY`, each exactly
`true` or `false`. Configure them in the protected publication environment only
after the corresponding runtime and deployment path has been qualified. They
persist the matching profile fields on later platform publications; they do not
activate deployment configuration. Harbor-only publications do not emit a
platform profile. Invalid values fail publication.

For the equivalent local preparation path, `nebius_candidate.py create` and
`build`, and `prepare_nebius_runtime_profile.py`, accept the optional flags
`--supports-task-web-egress`, `--service-lifecycle-ready` and
`--supports-task-identity`. Without them, the corresponding fields remain
omitted from the canonical profile. Web egress still requires the matching
[deployment policy](nebius-platform.md); the renderer rejects missing or
unmatched configuration. Identity readiness requires the explicit
[`task_identity_policy` deployment contract](nebius-platform.md#private-task-identity-policy).
Keep the variable disabled until the selected target passes installed
admission and execution qualification; successful image publication alone
does not establish that evidence.

BuildKit is pinned to `v0.33.0` and its manifest digest in the workflow.
Skopeo is pinned to Ubuntu package `1.13.3+ds1-2ubuntu0.24.04.3`; the explicit
Ubuntu 24.04 runner label keeps that package source stable. Its APT update and
install use only the runner's `/etc/apt/sources.list.d/ubuntu.sources`, retaining
the configured Ubuntu mirrors and native signature/checksum validation. Unrelated
vendor repositories cannot block this install; their files remain untouched.
This follows the upstream [runner source configuration](https://github.com/actions/runner-images/blob/main/images/ubuntu/scripts/build/configure-apt-sources.sh).
Trivy uses the
repository's version/hash-checked installer with no vulnerability exceptions;
Python tooling uses `uv sync --locked` with the existing `cluster` extra for the
Nebius SDK. Images are built to an OCI archive, scanned and copied with digest
preservation; registry readback must match the scanned immutable image. Initial
publication builds all seven platform/harness images without an incremental-release controller or
another required CI gate. The resulting platform images and all deployed
platform resources use Nebius; the build job itself uses GitHub infrastructure.

The execution-actuator image copies a selected set of shared modules. Its image
build imports both actuator and capacity-collector entrypoints as the runtime UID,
without credentials or external connections. Keep copied shared dependencies in
the image's component ownership so their changes rebuild the image. This catches
missing packaged imports before publication; deployment readiness still verifies
the actual database and Kubernetes connections.

## Publish and verify

The workflow runs on pushes to `dev` once enabled; it also supports
manual dispatch where GitHub exposes that event for the workflow. The
workflow's immutable `github.sha` is the candidate; there is no arbitrary candidate
override. The push trigger permits branch-local bootstrap without putting the
experimental workflow on `dev`. GitHub may suppress follow-on events for commits
created with a workflow's `GITHUB_TOKEN`; use a normal protected branch merge
initiated with the owner's supported GitHub credentials when publication is needed.
Do not substitute an ambient local publish or expand legacy lanes.

The artifact contains `candidate.json`, `runtime-profile.json`, and per-image scan
reports/SBOMs. Partial scan evidence is retained on failure; absence of the completed
candidate means no complete release exists. Partial registry uploads are possible
on failure and must not be deployed. Tag names aid discovery only: deployments
consume the published `@sha256:` references. Artifact names are
`nebius-candidate-<sha>-<run-id>-<run-attempt>` so retries retain earlier evidence
without upload-name conflicts. GitHub retains the run ID when rerunning a job;
download the exact attempt's artifact. Never resolve tags in an already accepted
candidate or overwrite an evidence bundle.

`candidate.json` is a plain publication record: repository/ref, commit, workflow
run and published image digest references. New platform publications also record
`source_archive_sha256`, measured from the actual `git archive --format=tar`
bytes at the exact clean candidate checkout. This is a committed-source archive
digest, not a hash of the commit identifier or an arbitrary-local-source upload.
The shared-data application upgrade requires this published value to match its
application release; older records without it remain valid for existing legacy
consumers, but cannot supply personal application provenance.
Renderer and deployer consume those fields without
detached signatures, profile hashes or another reconstruction/verification layer.
Optional format inspection is available with:

```sh
uv run --no-sync python scripts/ops/nebius_candidate.py check-shape \
  --candidate candidate.json
```

The publisher signs only the two image-admission statements already required by
Control Plane for the task/service image and execution-runtime image. Their
existing scan/provenance and runtime-binary fields remain in `runtime-profile.json`;
Control Plane verifies them through its established runtime admission path.
The signer/keyring remains environment-owned configuration. The publisher checks
the scanned OCI digest against the pushed digest once; later deployment steps
reuse the publication record instead of hashing the same files again.

Runtime admissions retain the existing non-expiring profile
contract; revoke a compromised signer through the independently managed keyring.
Vulnerability evidence belongs to its exact build and scan time and is not a
claim that the image remains free of subsequently discovered vulnerabilities.
Publication failures retain a bounded `failed-command.json` with operation,
exit status and sanitized diagnostics alongside partial scan evidence.

Required follow-up acceptance: provision the restricted GitHub environment and
publisher IAM/trust bindings, publish an exact merged integration commit, verify
registry readback, and deploy that publication bundle through the independent platform
renderer/deployer. Publication requires no cluster bootstrap.

The publisher exports one OCI archive per image. It extracts that archive into a temporary OCI layout directory for Trivy vulnerability and SBOM reports, then pushes the original archive with Skopeo. Trivy accepts an OCI layout directory through `--input`; an OCI tar archive is not a Docker-save archive. The temporary layout is removed after scanning. The existing scan policy and scanned-to-published digest check are unchanged.

## GitHub organization transfer

Loom's repository identity is GitHub repository ID `1281629473`. Candidate
publication and rollout workflow guards use this ID across the transfer from
`qianyi-sun/loom` to `ca-mrc/loom`. Candidate readers accept both names so that
previous publications remain usable; a different repository ID is rejected even
when its name matches. GitHub API readers use the numeric repository endpoint,
and artifact download commands resolve the current repository name.

Deploy the compatible service image before transferring the repository: older
installed candidate catalogs reject redirects from the former repository URL.
After transfer, verify the installed `loom-management-publications` token can
read the repository and candidate artifacts under the new organization. Preserve
existing registry image names and historical candidate records.
