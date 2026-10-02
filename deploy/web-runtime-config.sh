#!/usr/bin/env sh
set -eu

config_path="${LOOM_FRONTEND_CONFIG_PATH:-/usr/share/nginx/html/loom-frontend-config.json}"
index_template_path="${LOOM_FRONTEND_INDEX_TEMPLATE_PATH:-/usr/share/nginx/html/index.html.template}"
index_path="${LOOM_FRONTEND_INDEX_PATH:-/usr/share/nginx/html/index.html}"

environment="${LOOM_FRONTEND_ENVIRONMENT:-local}"
label="${LOOM_FRONTEND_ENVIRONMENT_LABEL:-Local development}"
route_path="${LOOM_FRONTEND_ROUTE_PATH:-}"
api_base="${LOOM_FRONTEND_API_BASE:-${route_path}}"
public_origin="${LOOM_FRONTEND_PUBLIC_ORIGIN:-}"
rehearsal_id="${LOOM_FRONTEND_REHEARSAL_ID:-}"

# #2009: "currently served build" identity — image-build-time facts, never
# deploy-manifest overridable, so a rollout's served config always reflects
# what this container actually shipped. build_time comes from a file
# because Dockerfile ENV can't hold a `date` computed at build time.
build_revision="${LOOM_FRONTEND_BUILD_REVISION:-unknown}"
source_ref="${LOOM_FRONTEND_SOURCE_REF:-unknown}"
build_kind="${LOOM_FRONTEND_BUILD_KIND:-unknown}"
source_digest="${LOOM_FRONTEND_SOURCE_DIGEST:-unknown}"
source_base_commit="${LOOM_FRONTEND_SOURCE_BASE_COMMIT:-unknown}"
if [ "${build_kind}" = "personal" ]; then
  build_revision="unknown"
fi
build_time_path="${LOOM_FRONTEND_BUILD_TIME_PATH:-/etc/loom-frontend-build-time}"
build_time=""
if [ -f "${build_time_path}" ]; then
  build_time="$(cat "${build_time_path}")"
fi

case "${environment}" in
  local|development|staging|production) ;;
  *)
    echo "LOOM_FRONTEND_ENVIRONMENT must be local, development, staging, or production" >&2
    exit 1
    ;;
esac

# The public route prefix is derived from the environment (path-prefix scheme):
# staging serves /staging, production /prod, development /dev, local none.
case "${environment}" in
  local) env_route="" ;;
  development) env_route="/dev" ;;
  staging) env_route="/staging" ;;
  production) env_route="/prod" ;;
esac

if [ -n "${rehearsal_id}" ]; then
  if [ "${environment}" != "staging" ] || ! printf '%s' "${rehearsal_id}" | grep -Eq '^[0-9a-f]{24}$'; then
    echo "LOOM_FRONTEND_REHEARSAL_ID requires staging and 24 lowercase hex characters" >&2
    exit 1
  fi
  if [ "${route_path}" != "${env_route}/rehearsal/${rehearsal_id}" ] || [ "${api_base}" != "${route_path}" ]; then
    echo "rehearsal frontend route must match its exact isolated identity" >&2
    exit 1
  fi
else
  case "${route_path}" in
    ""|"/"|"${env_route}") ;;
    *)
      echo "LOOM_FRONTEND_ROUTE_PATH must be empty, /, or ${env_route}" >&2
      exit 1
      ;;
  esac

  case "${api_base}" in
    ""|"/"|"${env_route}") ;;
    *)
      echo "LOOM_FRONTEND_API_BASE must be empty, /, or ${env_route}" >&2
      exit 1
      ;;
  esac
fi

if [ "${route_path}" = "/" ]; then
  route_path=""
fi
if [ "${api_base}" = "/" ]; then
  api_base=""
fi
if [ "${api_base}" != "${route_path}" ]; then
  echo "LOOM_FRONTEND_API_BASE must match LOOM_FRONTEND_ROUTE_PATH" >&2
  exit 1
fi
if [ -n "${public_origin}" ] && ! printf '%s' "${public_origin}" | grep -Eq '^https://[^/]+$'; then
  echo "LOOM_FRONTEND_PUBLIC_ORIGIN must be an https origin without a path" >&2
  exit 1
fi
if [ "${environment}" = "production" ] && [ "${route_path}" != "/prod" ]; then
  echo "production frontend must use LOOM_FRONTEND_ROUTE_PATH=/prod" >&2
  exit 1
fi
if [ "${environment}" != "production" ] && [ "${route_path}" = "/prod" ]; then
  echo "non-production frontend must not use LOOM_FRONTEND_ROUTE_PATH=/prod" >&2
  exit 1
fi
if [ "${environment}" = "production" ] && printf '%s' "${label}" | grep -Eiq 'beta'; then
  echo "production frontend label must not contain beta wording" >&2
  exit 1
fi

if [ ! -f "${index_template_path}" ]; then
  echo "frontend index template not found: ${index_template_path}" >&2
  exit 1
fi

tmp_index_path="${index_path}.tmp"
asset_path="${route_path}/assets/"
sed \
  -e "s|src=\"\./assets/|src=\"${asset_path}|g" \
  -e "s|href=\"\./assets/|href=\"${asset_path}|g" \
  "${index_template_path}" > "${tmp_index_path}"
if grep -Eq '(src|href)="\./assets/' "${tmp_index_path}"; then
  echo "frontend shell retained a relative build asset" >&2
  rm -f "${tmp_index_path}"
  exit 1
fi
if grep -Eq '/(dev|prod|staging)/(dev|prod|staging)/assets/' "${tmp_index_path}"; then
  echo "frontend shell contains a double or stale route prefix" >&2
  rm -f "${tmp_index_path}"
  exit 1
fi
mv "${tmp_index_path}" "${index_path}"

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

api_route_base="${public_origin}${api_base}/api"

tmp_path="${config_path}.tmp"
cat > "${tmp_path}" <<EOF
{
  "environment": "$(json_escape "${environment}")",
  "environmentLabel": "$(json_escape "${label}")",
  "routePath": "$(json_escape "${route_path}")",
  "apiBase": "$(json_escape "${api_base}")",
  "apiRouteBase": "$(json_escape "${api_route_base}")",
  "buildRevision": "$(json_escape "${build_revision}")",
  "sourceRef": "$(json_escape "${source_ref}")",
  "buildKind": "$(json_escape "${build_kind}")",
  "sourceDigest": "$(json_escape "${source_digest}")",
  "sourceBaseCommit": "$(json_escape "${source_base_commit}")",
  "buildTime": "$(json_escape "${build_time}")"
}
EOF
mv "${tmp_path}" "${config_path}"
