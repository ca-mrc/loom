#!/usr/bin/env bash
# Regenerate deploy/worker-image.lock and deploy/worker-image.wheels.json
# from a clean worker image build (#744 Gate 1).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
IMAGE="${LOOM_WORKER_IMAGE:-loom-worker:gate1-regen}"
DOCKERFILE="${ROOT}/deploy/Dockerfile.worker"

cd "${ROOT}"
docker build -f "${DOCKERFILE}" -t "${IMAGE}" .

LOCK="${ROOT}/deploy/worker-image.lock"
WHEELS="${ROOT}/deploy/worker-image.wheels.json"
TMP_HASH="$(mktemp)"

{
  echo "# Loom worker image pip freeze — regenerate via scripts/ops/update_worker_image_lock.sh"
  echo "# Image: ${IMAGE}"
  docker run --rm "${IMAGE}" pip freeze | LC_ALL=C sort
} > "${LOCK}"

docker run --rm "${IMAGE}" bash -c '
  set -euo pipefail
  cd /tmp
  OPENAI_VER="$(python -c "import importlib.metadata as m; print(m.version(\"openai\"))")"
  LITELLM_VER="$(python -c "import importlib.metadata as m; print(m.version(\"litellm\"))")"
  pip download "openai==${OPENAI_VER}" "litellm==${LITELLM_VER}" --no-deps -q
  pip hash *.whl
' > "${TMP_HASH}"

OPENAI_HASH="$(awk '/^openai-.*\.whl:/{getline; print}' "${TMP_HASH}" | sed 's/^--hash=sha256://')"
LITELLM_HASH="$(awk '/^litellm-.*\.whl:/{getline; print}' "${TMP_HASH}" | sed 's/^--hash=sha256://')"
OPENAI_VER="$(docker run --rm "${IMAGE}" python -c 'import importlib.metadata as m; print(m.version("openai"))')"
LITELLM_VER="$(docker run --rm "${IMAGE}" python -c 'import importlib.metadata as m; print(m.version("litellm"))')"
HARBOR_SHA="$(grep '^ARG HARBOR_COMPAT_SHA=' "${DOCKERFILE}" | cut -d= -f2)"
HARBOR_VERSION="$(docker run --rm "${IMAGE}" python -c 'import importlib.metadata as m; print(m.version("harbor"))')"

cat > "${WHEELS}" <<EOF
{
  "schema_version": "1",
  "python_version": "3.12",
  "harbor_compat_sha": "${HARBOR_SHA}",
  "harbor_runtime_version": "${HARBOR_VERSION}",
  "packages": {
    "openai": {
      "version": "${OPENAI_VER}",
      "wheel": "openai-${OPENAI_VER}-py3-none-any.whl",
      "sha256": "${OPENAI_HASH}"
    },
    "litellm": {
      "version": "${LITELLM_VER}",
      "wheel": "litellm-${LITELLM_VER}-py3-none-any.whl",
      "sha256": "${LITELLM_HASH}"
    }
  },
  "regenerate": "scripts/ops/update_worker_image_lock.sh"
}
EOF

rm -f "${TMP_HASH}"
echo "Updated ${LOCK} and ${WHEELS}"
docker run --rm "${IMAGE}" pip check
