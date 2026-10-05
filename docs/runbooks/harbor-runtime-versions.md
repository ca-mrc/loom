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
loom eval batch create --agent terminus-2 \
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
