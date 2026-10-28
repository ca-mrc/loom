# Select a published Harbor runtime

Native Nebius batches can select a published Terminus-2 controller version.
The task Dockerfile or image still supplies the task and verifier environments.
Changing the controller version does not require rebuilding those images or
redeploying the platform.

`GET /api/v1/agents` includes `versions` on each catalog item. The `terminus-2`
versions expose `agent_version`, `harbor_version`, `loom_bridge_revision`,
`readiness_status`, and `readiness_message`. An unavailable version remains
visible for historical selections but cannot start a new execution.
Use one of these case-sensitive version labels; arbitrary image URLs are not
accepted. Omit `agent_version` to keep the deployment default.

```sh
loom eval batch create --purpose evaluation --agent terminus-2 \
  --agent-version harbor-0.18.0-abc123 --provider my-provider \
  --model my-model --benchmark my-benchmark
```

The API equivalent puts `agent_version` in `trial_config`. For a comparison,
put the field on each item in `combinations` (or the CLI's
`--combinations-json`), alongside `agent_name` and `agent_model`. Shared
`trial_config.agent_version` cannot be combined with `combinations`. Generated
combination labels include the selected version.

Selection currently requires automatic native Nebius execution and
`agent_name: terminus-2`; explicitly templated execution and other backends
reject it. At submission the service resolves every selected label once and
freezes its controller image and release metadata in the Batch runtime profile.
Attempts and failed-case reruns retain that profile, even if the deployment
default or the catalog subsequently changes. A new Batch resolves the catalog
again. The existing image-admission records are reused and deduplicated within
the profile. No new admission scheme is introduced.

Known published bridges that compare the entire sandbox `/health` response
to `{"ready": true}` are unavailable (#2337). The response includes
`instance_id`, which must remain available for sandbox incarnation checks.
Compatibility is derived from immutable publisher/bridge source identity,
not the Harbor package version, release label, or shared `1.0` bridge label.
The catalog, submission, frozen failed-case rerun, and plan compilation use
the same compatibility rule. API submissions and reruns reject the old bridge
with an actionable reason before creating execution; historical bindings and
Trial results are not rewritten, and there is no silent default substitution.

Publish a replacement with the current compatible bridge using the existing
`nebius-candidate` workflow's `harness-only` mode and a new `agent_version`.
Register its original release JSON as described below. Qualify the replacement
through an ordinary-member exact-version submission with real agent calls,
independent verifier output, canonical download, and resource cleanup.
Publishing/registering an image alone does not establish that acceptance.

Task environment MCP servers and skills require the qualified combination of
Harbor `0.24.0` and Loom bridge `1.1`. The publisher derives the deployment
default's capability from the controller image's existing runtime labels;
explicit version selections use their frozen release metadata. Unknown
combinations and older bridges cannot run tasks requiring these fields, while
remaining eligible for ordinary tasks subject to the existing compatibility
checks. A catalog `ready` status does not imply support for every task feature.
Oracle follows the upstream utility behavior and ignores these task metadata
fields. Historical release JSON and signatures are not rewritten.

## Register a release

Publish the controller image and retain the publisher's
`agent-runtime-release.json`. An operator registers that exact file through the
Control Plane with the existing admin credential source:

```sh
loom admin agent-runtime register --cp-url https://control-plane.example \
  --admin-token env:LOOM_ADMIN_TOKEN --release agent-runtime-release.json
```

This calls `PUT /admin/agents/terminus-2/versions/{agent_version}` and requires
`admin:tokens`. The release contains schema
`loom.agent-runtime-release.v1`, runtime contract
`loom.terminus-controller.v1`, an immutable `agent_image_ref`, Harbor package
version and source revision, Loom bridge and publisher source revisions, and
the publisher's existing `image_admission` for that image. Source revisions are
40 lowercase hexadecimal characters. Labels match
`[A-Za-z0-9][A-Za-z0-9._-]{0,127}` and identify the whole controller release,
including Loom changes, rather than only the upstream Harbor version.

The Control Plane verifies the existing admission and stores the release under
the Agent's `(name, version)` key. Replaying the identical file is idempotent.
Rebinding a label to different contents returns HTTP 409, and generic catalog
provisioning cannot overwrite a registered release. Preserve the original JSON
for a retry; generating a new admission timestamp/signature changes the record.

The controller reports its installed Harbor package version and image-baked
source/bridge metadata. Task environment variables and uploaded inputs do not
choose those values. Canonical ATIF output and accounting repair preserve the
Trial's selected release label; older Trials retain their previous fallback.

Historical `raw-harbor-tb2-v2` downloads validate an explicitly selected version
against the Batch's frozen runtime binding, without consulting the mutable
catalog. A missing or mismatched binding fails closed. Older default runs did
not freeze a Harbor source revision; their v2 serialized format remains supported
for the previously qualified 0.18 source
`527d50deb63a5d279e8c20593c18a2cbc7f61f9e` and the active source only. Unknown
or mixed source revisions still fail, as do legacy streams, invalid joins and
artifact integrity failures. This download compatibility does not make an old
runtime available for new execution or change any Batch binding.

A Trial that selects no version (`terminus-2@default`) runs the platform
candidate's default controller image. The candidate's runtime profile records
that image's label as `agent_image_version` (`nebius-<candidate>` unless the
publication set `agent_version`). ATIF and accounting repair record that label
when the attempt ran on the Batch's frozen candidate. Profiles published before
this field, and attempts whose candidate differs, keep `service-execution-v1`.

## Refresh an upstream upgrade candidate

The `Harbor upstream candidate` workflow maintains one Draft on
`bot/harbor-upstream`. A clean refresh merges current `dev` into the existing
candidate, preserving human amendments, before updating explicit Harbor pins.
A Ready candidate is left unchanged. Offline conformance and continuation
qualification runs only after preparation publishes the candidate successfully.

Substantive merge conflicts require a maintainer to reconcile the existing
Draft. The workflow aborts its local merge, reports `refresh_status=action_required`
with both commit IDs and the conflicting paths, and fails. It does not push,
rewrite the Draft, or run offline qualification. The Draft remains unaccepted;
the reported conflict is an outstanding upgrade task. A Git/tooling failure
without content conflicts reports `refresh_status=failed` separately.

Resolve each conflict on the existing candidate branch while retaining its
reviewed compatibility changes. Frozen worker dependency evidence and provenance
tests need deliberate reconciliation and regeneration through
`scripts/ops/update_worker_image_lock.sh`; choosing an entire side of those files
cannot establish compatibility. Push the reviewed resolution normally, then
rerun candidate preparation and inspect offline qualification before completing
the remaining developer verification and marking the Draft Ready. Updater
diagnostics alone do not accept a Harbor upgrade or publish a runtime release.
