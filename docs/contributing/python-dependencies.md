# Python dependency qualification

Use the root `uv.lock` for application and development installs. Preserve its
Linux x86_64, Linux aarch64 and macOS arm64 resolution targets. Check selected
extras as well as the universal lock: optional GPU dependencies are not
automatically present in hosted applications. Image-specific locks and benchmark
task installers are separate graphs and need their own qualification.

## Client and test-tool migrations

The October 2026 maintenance changes replace unsupported `google-generativeai`
with `google-genai` 2.30.1. Local Google calls use its real asynchronous API;
transport tests cover request serialization, errors, usage, blocked responses
and client cleanup. These tests do not establish live provider acceptance.

The development graph uses pytest 9.0.3 and pytest-asyncio 1.3.0 across the root
and sibling extras. They replace pytest 8.4.2, whose temporary-directory handling
matches [CVE-2025-71176](https://osv.dev/vulnerability/GHSA-6w46-j5rx-g56g).
Keep existing fixture scopes, `asyncio_mode`, markers and CI test selection;
exercise async teardown and database fixtures when updating these tools.

OAuthlib 4.0.0 supports the Kubernetes 31 / requests-oauthlib 2 client path,
including token refresh. The previous version's
[PKCE advisory](https://osv.dev/vulnerability/GHSA-xpv3-w29h-x7cv) concerns an
OAuth authorization server; Loom uses the client path. OAuthlib is absent from
the default root install but present in `cluster`, which the service image also
selects. Treat image contents separately from default package contents.

## Optional GPU graph

The optional `loom[vllm]` graph remains tracked in
[#2488](https://github.com/ca-mrc/loom/issues/2488). The October 14, 2026 advisory
review found 71 distinct package/advisory matches in this graph after the
test-tool and OAuth updates. These are version matches, not 71 demonstrated
reachable vulnerabilities. Normal application images do not install this extra.

| Locked package | Remaining qualification |
| --- | --- |
| vLLM 0.11.0 | Model loading, structured output and several API/configuration paths have advisory matches. Loopback binding does not make untrusted model files safe. |
| PyTorch 2.8.0 | vLLM pins this version exactly. Model/embedding loading can reach unsafe loader paths; an independent update breaks the declared runtime combination. |
| Transformers 4.57.6 | Some reports concern training/conversion helpers absent from the managed inference flow; model-configuration loading still needs qualification. |
| xgrammar 0.1.25 | Structured-output grammar parsing is reachable upstream. The recursion advisory is fixed in 0.1.32, but vLLM pins 0.1.25 exactly. |
| diskcache 5.6.3 | The cache-deserialization advisory has no reported fix. Upstream's outlines disk cache is disabled by default; if enabled, its files must not be writable by untrusted tasks. |
| setuptools 79.0.1 | The advisory concerns macOS filename normalization and `MANIFEST.in` exclusions during sdist production. Loom has no `MANIFEST.in`. This optional runtime pin is distinct from the isolated build backend requirement. |

Do not override exact ML pins just to make a scanner green. Metadata-only
resolutions of intermediate vLLM versions retain advisories and may change
CUDA 12 to CUDA 13. Linux wheel availability also varies with architecture and
glibc. The reviewed vLLM 0.30 candidate requires PyTorch 2.13, CUDA 13,
Transformers >=5.10.4 and Hugging Face Hub >=1.31; the application currently bounds
Hub below 1. It still caps setuptools below the advisory's fixed version on
Python above 3.11. A successful resolution does not establish GPU execution or
benchmark compatibility.

A runtime migration must name its OS/glibc, GPU/driver/CUDA and model matrix;
qualify model loading, chat/tool/structured-output requests, cancellation and
benchmark behavior; then re-audit the graph. Keep default/dev platforms usable.
The separate cryptography and fsspec/datasets work is tracked in
[#2487](https://github.com/ca-mrc/loom/issues/2487).

## Repeatable checks

Use `uv export --frozen` to inspect default, development, cluster, rollout and
optional installs independently. Query exact locked package/version entries,
including alternate Python/platform branches, and deduplicate advisory aliases.
For each match, record the consumer, affected operation, fixed candidate or
no-fix status, and untested boundary. A zero-match result is a dated database
observation, not proof of safety under every input.

After updating a lock, run `uv lock --check`, sync the selected workspace and
run `uv pip check`. Exercise the actual SDK/interface or runtime boundary and
the checks required by [CONTRIBUTING.md](../../CONTRIBUTING.md). Retain audit
output outside the repository; put durable compatibility guidance here and
unresolved work in its owning issue.
