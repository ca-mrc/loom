# Real Harbor Terminus-2 conformance

The production `deploy/Dockerfile.harbor-runtime` installs the revision in
[`config/harbor-runtime.json`](../../../config/harbor-runtime.json). The image
validation workflow runs all three probes below inside that built image with
`--network none`. Missing Harbor or a mismatched installed revision fails;
there is no optional-import skip in this lane.

- [`terminus_conformance_probe.py`](../../support/terminus_conformance_probe.py)
  compares the real upstream parser, Chat, agent loop and native trajectory
  writer with `LoomTerminus2Runtime`. Only model and terminal transports are
  scripted. It checks full request histories, command batches, terminal
  observations, parser retries, completion confirmation, explicit turn limits,
  command timeouts, terminal model errors, usage and typed-event projection.
  Deliberate prompt, command and sampling drift must be rejected. A separate
  case uses the real Harbor/LiteLLM constructors to check teacher options,
  Gateway credential reuse and trajectory-metadata redaction without issuing
  model requests.
- [`terminus_continuation_probe.py`](../../support/terminus_continuation_probe.py)
  verifies Loom's explicit continuation extension, cancellation, concurrent
  instance isolation and trajectory size limits with the real Harbor loop.

- [`terminus_dispatch_diagnostics_probe.py`](../../support/terminus_dispatch_diagnostics_probe.py)
  exercises parsed-command checkpoints, tmux dispatch diagnostics and dead-session
  refusal without replay. The existing image job adds tmux to a disposable tool
  fixture derived from the production controller; runtime probes stay offline.

The direct-provider Docker lane additionally uses real Harbor/LiteLLM with the
root's fixed provider SDKs for OpenAI, Anthropic, Google and localhost transports,
usage, retry and cancellation. It mounts the canonical Harbor manifest read-only
alongside the test's repository layout, so the expected version follows the
reviewed pin rather than the installed package. Its source revision assertion
remains strict. Python 3.11's optional-import skip is not this qualification.

This proves compatibility for **the same effective options**. It does not prove
upstream-default parity: Loom currently uses 50 turns and disables summaries.
It also does not exercise the real provider, terminal transport, verifier,
download or resource lifecycle. Those require separate runtime acceptance.

Local verification (Docker required, no model credentials):

```bash
uv sync --locked --all-packages --extra dev --python 3.11
uv run --no-sync pytest tests/integration/test_terminus2_continuation_docker.py \
  tests/integration/test_terminus2_direct_provider_docker.py -q
```

The continuation test builds the production image; the provider lane also verifies
the configured pin. Run dispatch diagnostics with the existing tmux fixture step
in [images.yml](../../../.github/workflows/images.yml). Set `LOOM_TEST_HARBOR_IMAGE` only to reuse
a known pinned dependency image; local current Loom sources are still mounted
read-only. CI always exercises the newly built candidate image.

See [the upgrade workflow](../../../docs/contributing/terminus2-upgrades.md)
for pin synchronization, existing deviations and acceptance requirements.
