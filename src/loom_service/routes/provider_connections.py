"""Provider-connection CRUD (cluster-deploy.md §CLI surface + §Schema).

Routes:
- POST   /api/v1/provider-connections       — create + encrypt + store
- GET    /api/v1/provider-connections       — list (team-scoped; soft-deleted excluded)
- GET    /api/v1/provider-connections/{id}  — detail
- PATCH  /api/v1/provider-connections/{id}  — update (base_url, api_key, allowed_models, pricing)
- DELETE /api/v1/provider-connections/{id}  — soft-delete (sets deleted_at)

`test` + `models` routes land in a follow-up PR alongside the
provider_models_cache refresh contract.

Trust boundary:
- All routes require team-or-admin auth; cross-team access returns 404
  (not 403) to avoid leaking existence.
- Secret values (api_key) live in the SecretStore (loom-encrypted by
  default); the row stores only the opaque ref. Responses NEVER include
  the api_key — it's write-only.
- SSRF defense layer 3 (DNS-resolve + classify) gates creation; layers
  1+2+4 (sandbox isolation + step-JWT auth + egress proxy enforcement)
  ship separately.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit
from uuid import UUID, uuid4

from fastapi import APIRouter, Header, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import delete, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from loom.auth import AuthContext
from loom.db.schema import (
    ProviderConnection,
    ProviderConnectionShare,
    ProviderModelCache,
    Team,
    TeamQuota,
)
from loom.provider_pricing import ModelPrice
from loom.security.secret_store import (
    InvalidRefError,
    LocalEncryptedSecretStore,
    SecretStore,
    SecretStoreError,
)
from loom_service.admin_audit import require_admin_actor, write_admin_audit_event
from loom_service.auth_guards import is_admin, require_scope
from loom_service.dependencies import SessionAndCtx
from loom_service.provider_connections_service import (
    InvalidBaseUrlError,
    ModelPreflightResult,
    ProbeResult,
    SsrfRejectedError,
    UpstreamModelFetchError,
    fetch_upstream_models,
    preflight_failure_kind,
    preflight_model,
    probe_connection,
    resolve_and_validate,
)
from loom_service.provider_model_classifier import classify_model_id
from loom_service.provider_pricing import (
    apply_config,
    ensure_catalog_share_compatible,
    pricing_response,
    resolve_config,
)
from loom_service.provider_secret_gc import retire_provider_secret

router = APIRouter()


# ──────────────────────────────────────────────────────────────────────
# Pydantic models
# ──────────────────────────────────────────────────────────────────────


# Matches the DB CHECK constraint domain. Kept here for the
# enum-in-error-message path; the actual validation against this set
# happens at the DB layer via the migration's CHECK constraint, so
# we don't need to re-list it in Pydantic-Literal form.
_PROVIDER_TYPES = ("openai-compatible", "anthropic", "google", "custom")
_PROVIDER_SECRET_UNREADABLE_DETAIL = (
    "stored provider secret cannot be decrypted or read. Restore the "
    "SecretStore master key/fallback key, run secret-store rewrap if a "
    "rotation is in progress, or rotate/re-enter the provider API key."
)


class ProviderConnectionCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=128)
    type: str = Field(description=f"one of {_PROVIDER_TYPES}")
    base_url: str = Field(min_length=1)
    # api_key is write-only — never returned in responses. Accepts the
    # raw secret; the route encrypts via SecretStore before persisting.
    api_key: str = Field(min_length=1)
    allowed_models: list[str] | None = Field(default=None)
    pricing_mode: Literal["usage_only", "catalog", "custom"] | None = None
    custom_pricing: dict[str, ModelPrice] | None = None
    supplier_id: str | None = Field(default=None, max_length=128)
    catalog_id: str | None = Field(default=None, max_length=256)


class ProviderConnectionUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # All fields optional — PATCH semantics. `name` and `type` are NOT
    # updatable (changing them changes which connection consumers
    # reference); operators who need a new name/type create a new
    # connection and migrate. base_url IS updatable (provider URL
    # changes are common) — the route re-derives upstream_host and
    # re-resolves IPs.
    base_url: str | None = None
    api_key: str | None = None
    allowed_models: list[str] | None = None
    pricing_mode: Literal["usage_only", "catalog", "custom"] | None = None
    custom_pricing: dict[str, ModelPrice] | None = None
    supplier_id: str | None = Field(default=None, max_length=128)
    catalog_id: str | None = Field(default=None, max_length=256)


class ProviderConnectionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    team_id: UUID
    name: str
    type: str
    base_url: str
    upstream_host: str
    resolved_egress_ips: list[str]
    allowed_models: list[str] | None
    status: str
    last_validated_at: datetime | None
    last_validation_error: str | None
    pricing_mode: Literal["usage_only", "catalog", "custom"]
    custom_pricing: dict[str, ModelPrice] | None
    supplier_id: str | None
    catalog_id: str | None
    legacy_pricing: dict[str, Any] | None
    created_by: str
    created_at: datetime
    updated_at: datetime


class ProviderConnectionListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[ProviderConnectionResponse]


class ProviderConnectionShareRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_team_id: UUID


class ProviderConnectionShareResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    provider_connection_id: UUID
    provider_name: str
    provider_owner_team_id: UUID
    target_team_id: UUID


class ProviderConnectionTestResponse(BaseModel):
    """Result of POST /provider-connections/{id}/test.

    Mirrors the row columns updated by the route so the CLI can render
    a useful summary without a second GET. `http_status=None` means the
    request never reached an HTTP server (DNS, connect, timeout).
    """

    model_config = ConfigDict(extra="forbid")

    connection_id: UUID
    status: str  # 'valid' | 'invalid'
    http_status: int | None
    last_validation_error: str | None
    last_validated_at: datetime


class ProviderModelCacheEntry(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_id: str
    family: str | None
    context_length: int | None
    capabilities: dict[str, object]
    visible: bool
    hidden_reason: str | None
    last_seen_at: datetime
    upstream_present: bool
    source: str
    agent_capable: bool
    recommended: bool
    visibility: str
    last_preflight_status: str | None
    last_preflight_at: datetime | None
    last_preflight_http_status: int | None
    last_preflight_error_code: str | None
    last_preflight_error_message: str | None
    # 'rejected' blocks batch submission; 'inconclusive' (timeout, network
    # error, 408/429/5xx) does not (#948). None unless the preflight failed.
    last_preflight_failure_kind: Literal["rejected", "inconclusive"] | None = None


class ProviderModelCacheListResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[ProviderModelCacheEntry]


class ProviderModelRefreshResponse(BaseModel):
    """Summary of a /models/refresh: counts and the resulting cache
    state. Counts are computed against the cache BEFORE the upsert."""

    model_config = ConfigDict(extra="forbid")

    added: int
    refreshed: int
    missing: int
    items: list[ProviderModelCacheEntry]


class ProviderModelManualCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    model_id: str = Field(min_length=1, max_length=512)
    family: str | None = Field(default=None, max_length=128)
    context_length: int | None = Field(default=None, gt=0)
    capabilities: dict[str, object] = Field(default_factory=dict)


class ProviderModelMetadataUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    context_length: int = Field(gt=0, le=2_000_000)
    specification_url: str = Field(min_length=1, max_length=2048, pattern=r"^https://[^\s]+$")

    @field_validator("specification_url")
    @classmethod
    def public_specification(cls, value: str) -> str:
        parsed = urlsplit(value)
        if (
            not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "provide a public HTTPS specification URL without credentials or query"
            )
        return value


# ──────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────


def _cache_row_to_entry(row: ProviderModelCache) -> ProviderModelCacheEntry:
    capabilities = dict(row.capabilities or {})
    source = capabilities.get("source")
    source_str = source if isinstance(source, str) else "discovered"
    classification = classify_model_id(
        row.model_id, source=source_str, family=row.family,
    )
    if not row.visible:
        visibility = "hidden"
        recommended = False
        hidden_reason = row.hidden_reason
    else:
        visibility = classification.visibility
        recommended = classification.recommended
        hidden_reason = row.hidden_reason or classification.reason
    return ProviderModelCacheEntry(
        model_id=row.model_id,
        family=row.family,
        context_length=row.context_length,
        capabilities=capabilities,
        visible=row.visible,
        hidden_reason=hidden_reason,
        last_seen_at=row.last_seen_at,
        upstream_present=row.upstream_present,
        source=source_str,
        agent_capable=classification.agent_capable,
        recommended=recommended,
        visibility=visibility,
        last_preflight_status=row.last_preflight_status,
        last_preflight_at=row.last_preflight_at,
        last_preflight_http_status=row.last_preflight_http_status,
        last_preflight_error_code=row.last_preflight_error_code,
        last_preflight_error_message=row.last_preflight_error_message,
        last_preflight_failure_kind=preflight_failure_kind(
            row.last_preflight_status,
            row.last_preflight_error_code,
            row.last_preflight_http_status,
        ),
    )


def _is_manual_model(row: ProviderModelCache) -> bool:
    source = (row.capabilities or {}).get("source")
    return source == "manual"


def _row_to_response(row: ProviderConnection) -> ProviderConnectionResponse:
    return ProviderConnectionResponse(
        id=row.id,
        team_id=row.team_id,
        name=row.display_name,
        type=row.provider_type,
        base_url=row.base_url,
        upstream_host=row.upstream_host,
        # Postgres inet[] comes back as a list of `ipaddress.IPvNAddress`
        # objects via SA; coerce to plain strings for the response.
        resolved_egress_ips=[str(ip) for ip in (row.resolved_egress_ips or [])],
        allowed_models=row.allowed_models,
        status=row.status,
        last_validated_at=row.last_validated_at,
        last_validation_error=row.last_validation_error,
        **pricing_response(row),
        created_by=row.created_by,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _make_secret_store(session: AsyncSession) -> SecretStore:
    """Construct a SecretStore from the request's session.

    Today: always `LocalEncryptedSecretStore` (the data-path default
    per cluster-deploy.md). When the k8s-secret backend lands, this
    dispatches by an env var or per-deployment-mode config. The route
    code doesn't care which backend — it gets back an object satisfying
    the SecretStore Protocol.
    """
    return LocalEncryptedSecretStore(session)


def _provider_audit_actor(
    ctx: AuthContext,
    x_loom_admin_actor: str | None,
) -> str:
    if ctx.type == "admin":
        return require_admin_actor(x_loom_admin_actor)
    if is_admin(ctx):
        if ctx.user_id is not None:
            return f"user:{ctx.user_id}"
        return "platform_admin"
    token_prefix = ctx.token_hash.hex()[:16] if ctx.token_hash else "none"
    return f"{ctx.type}:{token_prefix}"


def _provider_audit_metadata(
    row: ProviderConnection,
    *,
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    metadata: dict[str, object] = {
        "team_id": str(row.team_id),
        "provider_type": row.provider_type,
        "display_name": row.display_name,
    }
    if extra:
        metadata.update(extra)
    return metadata


async def _get_provider_api_key(
    session: AsyncSession,
    row: ProviderConnection,
) -> str:
    secret_store = _make_secret_store(session)
    try:
        return await secret_store.get(row.encrypted_api_key_ref)
    except InvalidRefError as exc:
        # The stored ref doesn't parse as `loom://<namespace>/<uuid>` —
        # almost always a legacy row created before secret-store
        # enforcement (#423). Decrypt errors and missing-secret errors
        # can be transient (master-key rotation, etc.) and shouldn't
        # poison row.status, but a malformed ref is structurally broken
        # and requires operator repair, so flip status='invalid' so
        # listing surfaces no longer hide it behind status='valid'.
        row.status = "invalid"
        row.last_validation_error = (
            f"malformed_ref: {exc}. Operator must run "
            f"`loom providers rotate-key` to re-register the api_key."
        )
        row.last_validated_at = datetime.now(UTC)
        await session.commit()
        raise HTTPException(
            status_code=503,
            detail=_PROVIDER_SECRET_UNREADABLE_DETAIL,
        ) from exc
    except SecretStoreError as exc:
        raise HTTPException(
            status_code=503,
            detail=_PROVIDER_SECRET_UNREADABLE_DETAIL,
        ) from exc


async def _get_active_connection(
    session: AsyncSession, connection_id: UUID, ctx: AuthContext,
    *, for_update: bool = False,
) -> ProviderConnection:
    """Lookup a connection visible to `ctx`. Returns 404 for nonexistent
    / soft-deleted / cross-team rows alike — same response so existence
    can't be probed across team boundaries. Admins see any team's rows; team
    tokens see their own rows plus rows explicitly shared with their team."""
    stmt = select(ProviderConnection).where(
        ProviderConnection.id == connection_id,
        ProviderConnection.deleted_at.is_(None),
    )
    if for_update:
        stmt = stmt.with_for_update().execution_options(populate_existing=True)
    row = (await session.execute(stmt)).scalar_one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="provider_connection not found")
    if is_admin(ctx) or row.team_id == ctx.team_id:
        return row
    shared = None
    if ctx.team_id is not None:
        shared = (await session.execute(
            select(ProviderConnectionShare.provider_connection_id).where(
                ProviderConnectionShare.provider_connection_id == row.id,
                ProviderConnectionShare.target_team_id == ctx.team_id,
            ),
        )).scalar_one_or_none()
    if shared is None:
        raise HTTPException(status_code=404, detail="provider_connection not found")
    return row


async def _get_cached_model_or_404(
    session: AsyncSession,
    connection_id: UUID,
    model_id: str,
) -> ProviderModelCache:
    cache_row = (await session.execute(
        select(ProviderModelCache).where(
            ProviderModelCache.provider_connection_id == connection_id,
            ProviderModelCache.model_id == model_id,
        ),
    )).scalar_one_or_none()
    if cache_row is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"model {model_id!r} not in cache for this connection — "
                f"run POST /models/refresh first or add it manually"
            ),
        )
    return cache_row


def _require_provider_management(ctx: AuthContext) -> None:
    """Gate provider credential/catalog mutations for all non-admin callers."""
    if not is_admin(ctx):
        require_scope(ctx, "providers:manage")


def _require_provider_owner_or_admin(
    ctx: AuthContext,
    row: ProviderConnection,
) -> None:
    if is_admin(ctx):
        return
    if ctx.team_id != row.team_id:
        raise HTTPException(status_code=404, detail="provider_connection not found")


def _share_response(
    row: ProviderConnection,
    target_team_id: UUID,
) -> ProviderConnectionShareResponse:
    return ProviderConnectionShareResponse(
        provider_connection_id=row.id,
        provider_name=row.display_name,
        provider_owner_team_id=row.team_id,
        target_team_id=target_team_id,
    )


async def _team_allows_private(session: AsyncSession, team_id: UUID) -> bool:
    """Read the team's allow_private_endpoints flag. Missing TeamQuota
    row = strict default (False). Routes that haven't seen the team
    before treat them as untrusted."""
    flag = (await session.execute(
        select(TeamQuota.allow_private_endpoints).where(
            TeamQuota.team_id == team_id,
        ),
    )).scalar_one_or_none()
    return bool(flag)


async def _refresh_current_egress_or_error(
    session: AsyncSession, row: ProviderConnection,
) -> str | None:
    """Re-check a stored provider URL before each service-side outbound call."""
    allow_private = await _team_allows_private(session, row.team_id)
    try:
        resolved = resolve_and_validate(row.base_url, allow_private=allow_private)
    except InvalidBaseUrlError as e:
        return f"base_url invalid: {e}"
    except SsrfRejectedError as e:
        return f"base_url rejected: {e}"

    row.upstream_host = resolved.upstream_host
    row.resolved_egress_ips = resolved.resolved_ips
    row.egress_ips_refreshed_at = datetime.now(UTC)
    return None


# ──────────────────────────────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────────────────────────────


@router.post(
    "/provider-connections",
    response_model=ProviderConnectionResponse,
    status_code=201,
)
async def create_connection(
    request: Request,
    payload: ProviderConnectionCreate,
    sc: SessionAndCtx,
    x_loom_admin_actor: str | None = Header(default=None),
) -> ProviderConnectionResponse:
    session, ctx = sc
    _require_provider_management(ctx)
    if ctx.team_id is None:
        raise HTTPException(
            status_code=400,
            detail="provider_connection creation requires a team-scoped token",
        )
    team_id = ctx.team_id

    if payload.type not in _PROVIDER_TYPES:
        raise HTTPException(
            status_code=400,
            detail=f"type must be one of {_PROVIDER_TYPES}",
        )

    pricing = await resolve_config(session, team_id, payload.model_dump(
        include={"pricing_mode", "custom_pricing", "supplier_id", "catalog_id"},
        exclude_unset=True, mode="json",
    ))

    # SSRF defense layer 3: resolve + classify the upstream IPs against
    # the team's policy. Failure here is a 400; the user can fix the
    # URL or ask admin to flip the team flag.
    allow_private = await _team_allows_private(session, team_id)
    try:
        resolved = resolve_and_validate(
            payload.base_url, allow_private=allow_private,
        )
    except InvalidBaseUrlError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except SsrfRejectedError as e:
        raise HTTPException(
            status_code=400,
            detail=f"base_url rejected: {e}",
        ) from e

    # Encrypt the api_key BEFORE inserting the connection row so we
    # never have a row with a non-existent ref. If put() succeeds and
    # the INSERT fails, the orphaned secret is cleaned up by the
    # session rollback (LocalEncryptedSecretStore.put adds to session
    # via SQLAlchemy add() + flush; rollback removes it).
    secret_store = _make_secret_store(session)
    namespace = f"team:{team_id}"
    encrypted_ref = await secret_store.put(
        namespace=namespace, value=payload.api_key,
    )

    row = ProviderConnection(
        id=uuid4(),
        team_id=team_id,
        provider_type=payload.type,
        display_name=payload.name,
        base_url=payload.base_url,
        upstream_host=resolved.upstream_host,
        resolved_egress_ips=resolved.resolved_ips,
        egress_ips_refreshed_at=datetime.now(UTC),
        encrypted_api_key_ref=encrypted_ref,
        allowed_models=payload.allowed_models,
        status="pending",  # /test route flips to 'valid' / 'invalid'
        pricing_source="tokens-only",
        pricing_config=pricing.model_dump(mode="json"),
        # Per spec audit format: "<token-type>:<token-id-suffix>".
        # token_hash is bytes (sha256 digest); hex-encode the prefix
        # for a stable printable suffix without leaking the full hash.
        created_by=f"{ctx.type}:{ctx.token_hash.hex()[:16]}",
    )
    session.add(row)
    try:
        await session.flush()
    except IntegrityError as e:
        # The partial-UNIQUE-on-active-name catches duplicates here.
        # Roll back the SecretStore.put too (rolled back by session
        # rollback in the request teardown path).
        await session.rollback()
        raise HTTPException(
            status_code=409,
            detail=(
                f"a provider_connection named {payload.name!r} already "
                f"exists for this team"
            ),
        ) from e
    await write_admin_audit_event(
        session,
        actor=_provider_audit_actor(ctx, x_loom_admin_actor),
        action="provider_connection.create",
        target_type="provider_connection",
        target_id=str(row.id),
        request=request,
        metadata=_provider_audit_metadata(
            row,
            extra={
                "pricing_mode": pricing.pricing_mode,
                "catalog_id": pricing.catalog_id,
                "allowed_models_present": payload.allowed_models is not None,
            },
        ),
    )
    await session.commit()

    return _row_to_response(row)


@router.get(
    "/provider-connections",
    response_model=ProviderConnectionListResponse,
)
async def list_connections(
    sc: SessionAndCtx,
    team_id: Annotated[UUID | None, Query()] = None,
) -> ProviderConnectionListResponse:
    session, ctx = sc
    if ctx.team_id is None and not is_admin(ctx):
        raise HTTPException(
            status_code=400,
            detail="provider_connection list requires team-scoped or admin token",
        )
    stmt = select(ProviderConnection).where(
        ProviderConnection.deleted_at.is_(None),
    )
    # Admins see all teams' connections, or can explicitly scope the
    # listing when resolving an on-behalf-of submission. Team tokens
    # see their own rows plus rows explicitly shared with their team.
    if team_id is not None:
        if not is_admin(ctx) and team_id != ctx.team_id:
            raise HTTPException(
                status_code=403,
                detail="cross-team provider listing requires admin scope",
            )
        shared_ids = select(ProviderConnectionShare.provider_connection_id).where(
            ProviderConnectionShare.target_team_id == team_id,
        )
        stmt = stmt.where(
            or_(
                ProviderConnection.team_id == team_id,
                ProviderConnection.id.in_(shared_ids),
            ),
        )
    elif not is_admin(ctx):
        assert ctx.team_id is not None
        shared_ids = select(ProviderConnectionShare.provider_connection_id).where(
            ProviderConnectionShare.target_team_id == ctx.team_id,
        )
        stmt = stmt.where(
            or_(
                ProviderConnection.team_id == ctx.team_id,
                ProviderConnection.id.in_(shared_ids),
            ),
        )
    stmt = stmt.order_by(ProviderConnection.created_at.desc())

    rows = (await session.execute(stmt)).scalars().all()
    return ProviderConnectionListResponse(
        items=[_row_to_response(r) for r in rows],
    )


@router.get(
    "/provider-connections/{connection_id}",
    response_model=ProviderConnectionResponse,
)
async def get_connection(
    connection_id: UUID, sc: SessionAndCtx,
) -> ProviderConnectionResponse:
    session, ctx = sc
    row = await _get_active_connection(session, connection_id, ctx)
    return _row_to_response(row)


@router.post(
    "/provider-connections/{connection_id}/shares",
    response_model=ProviderConnectionShareResponse,
    status_code=201,
)
async def share_connection(
    request: Request,
    connection_id: UUID,
    payload: ProviderConnectionShareRequest,
    sc: SessionAndCtx,
    x_loom_admin_actor: str | None = Header(default=None),
) -> ProviderConnectionShareResponse:
    session, ctx = sc
    _require_provider_management(ctx)
    row = await _get_active_connection(session, connection_id, ctx, for_update=True)
    _require_provider_owner_or_admin(ctx, row)
    if row.pricing_config and row.pricing_config.get("catalog_id"):
        from loom.db.schema import PriceCatalog
        catalog = await session.get(PriceCatalog, row.pricing_config["catalog_id"])
        if catalog is not None and catalog.team_id is not None:
            raise HTTPException(400, "Connections using team-private price catalogs cannot be shared across teams.")


    if payload.target_team_id == row.team_id:
        raise HTTPException(
            status_code=400,
            detail="provider owner team already has access",
        )
    target_team_exists = (await session.execute(
        select(Team.id).where(Team.id == payload.target_team_id),
    )).scalar_one_or_none()
    if target_team_exists is None:
        raise HTTPException(status_code=404, detail="target team not found")

    actor = _provider_audit_actor(ctx, x_loom_admin_actor)
    stmt = pg_insert(ProviderConnectionShare).values(
        provider_connection_id=row.id,
        target_team_id=payload.target_team_id,
        created_by_actor=actor,
        created_by_user_id=ctx.user_id,
    )
    stmt = stmt.on_conflict_do_nothing(
        index_elements=[
            ProviderConnectionShare.provider_connection_id,
            ProviderConnectionShare.target_team_id,
        ],
    )
    await session.execute(stmt)
    await write_admin_audit_event(
        session,
        actor=actor,
        action="provider_connection.share",
        target_type="provider_connection",
        target_id=str(row.id),
        request=request,
        metadata={
            "provider_connection_id": str(row.id),
            "provider_name": row.display_name,
            "provider_owner_team_id": str(row.team_id),
            "target_team_id": str(payload.target_team_id),
            "action": "share",
        },
    )
    await session.commit()
    return _share_response(row, payload.target_team_id)


@router.delete(
    "/provider-connections/{connection_id}/shares/{target_team_id}",
    status_code=204,
)
async def unshare_connection(
    request: Request,
    connection_id: UUID,
    target_team_id: UUID,
    sc: SessionAndCtx,
    x_loom_admin_actor: str | None = Header(default=None),
) -> None:
    session, ctx = sc
    _require_provider_management(ctx)
    row = await _get_active_connection(session, connection_id, ctx)
    _require_provider_owner_or_admin(ctx, row)

    actor = _provider_audit_actor(ctx, x_loom_admin_actor)
    await session.execute(
        delete(ProviderConnectionShare).where(
            ProviderConnectionShare.provider_connection_id == row.id,
            ProviderConnectionShare.target_team_id == target_team_id,
        ),
    )
    await write_admin_audit_event(
        session,
        actor=actor,
        action="provider_connection.unshare",
        target_type="provider_connection",
        target_id=str(row.id),
        request=request,
        metadata={
            "provider_connection_id": str(row.id),
            "provider_name": row.display_name,
            "provider_owner_team_id": str(row.team_id),
            "target_team_id": str(target_team_id),
            "action": "unshare",
        },
    )
    await session.commit()
    return None


@router.patch(
    "/provider-connections/{connection_id}",
    response_model=ProviderConnectionResponse,
)
async def update_connection(
    request: Request,
    connection_id: UUID,
    payload: ProviderConnectionUpdate,
    sc: SessionAndCtx,
    x_loom_admin_actor: str | None = Header(default=None),
) -> ProviderConnectionResponse:
    session, ctx = sc
    _require_provider_management(ctx)
    if ctx.team_id is None and not is_admin(ctx):
        raise HTTPException(
            status_code=400,
            detail="PATCH requires team-scoped or admin token",
        )
    row = await _get_active_connection(session, connection_id, ctx, for_update=True)
    _require_provider_owner_or_admin(ctx, row)
    changed_fields: list[str] = []

    # base_url change → re-derive upstream_host + re-resolve IPs.
    # PATCH validates against the OWNER team's allow_private flag,
    # not the caller's (admin updating another team's row uses that
    # team's policy).
    if payload.base_url is not None:
        changed_fields.append("base_url")
        allow_private = await _team_allows_private(session, row.team_id)
        try:
            resolved = resolve_and_validate(
                payload.base_url, allow_private=allow_private,
            )
        except InvalidBaseUrlError as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        except SsrfRejectedError as e:
            raise HTTPException(
                status_code=400, detail=f"base_url rejected: {e}",
            ) from e
        row.base_url = payload.base_url
        row.upstream_host = resolved.upstream_host
        row.resolved_egress_ips = resolved.resolved_ips
        row.egress_ips_refreshed_at = datetime.now(UTC)
        # Connection needs re-validation against the new base_url; the
        # `test` route (Phase 2 follow-up) will flip back to 'valid'.
        row.status = "pending"

    pricing_fields = {"pricing_mode", "custom_pricing", "supplier_id", "catalog_id"}
    if payload.model_fields_set & pricing_fields:
        changes = payload.model_dump(include=pricing_fields, exclude_unset=True, mode="json")
        # Existing legacy monetary settings require an explicit complete choice;
        # editing endpoint/models alone leaves them untouched.
        if row.pricing_config is None and row.pricing_source != "tokens-only" and "pricing_mode" not in changes:
            raise HTTPException(400, "select a pricing mode to replace legacy pricing")
        config = await resolve_config(session, row.team_id, changes, previous=row.pricing_config)
        await ensure_catalog_share_compatible(session, row, config)
        apply_config(row, config)
        changed_fields.append("pricing")

    if payload.allowed_models is not None:
        changed_fields.append("allowed_models")
        row.allowed_models = payload.allowed_models

    # API key rotation encrypts the new value and swaps the active ref.
    # Retirement is committed atomically with the swap; GC observes a full grace.
    if payload.api_key is not None:
        changed_fields.append("api_key")
        secret_store = _make_secret_store(session)
        new_ref = await secret_store.put(
            namespace=f"team:{row.team_id}", value=payload.api_key,
        )
        await retire_provider_secret(session, ref=row.encrypted_api_key_ref, team_id=row.team_id)
        row.encrypted_api_key_ref = new_ref
        row.status = "pending"

    await session.flush()  # trigger touches updated_at
    await write_admin_audit_event(
        session,
        actor=_provider_audit_actor(ctx, x_loom_admin_actor),
        action="provider_connection.update",
        target_type="provider_connection",
        target_id=str(row.id),
        request=request,
        metadata=_provider_audit_metadata(
            row,
            extra={
                "changed_fields": sorted(set(changed_fields)),
                "api_key_rotated": payload.api_key is not None,
            },
        ),
    )
    await session.commit()
    return _row_to_response(row)


@router.post(
    "/provider-connections/{connection_id}/test",
    response_model=ProviderConnectionTestResponse,
)
async def test_connection(
    request: Request,
    connection_id: UUID,
    sc: SessionAndCtx,
    x_loom_admin_actor: str | None = Header(default=None),
) -> ProviderConnectionTestResponse:
    """Probe the configured base_url with the stored (decrypted) api_key.

    Decrypts the api_key from SecretStore, issues a single short-timeout
    GET to the provider's `/models` endpoint (`?key=...` for google),
    and persists the outcome on the row: `status` ∈ {'valid', 'invalid'},
    `last_validated_at`, `last_validation_error`. Redirects are NOT
    followed — a 3xx to an internal host would re-introduce SSRF
    despite the create-time IP validation.

    The api_key never leaves loom_service; it is decrypted in-process
    and passed only in the probe request's headers.
    """
    session, ctx = sc
    _require_provider_management(ctx)
    row = await _get_active_connection(session, connection_id, ctx)
    _require_provider_owner_or_admin(ctx, row)

    policy_error = await _refresh_current_egress_or_error(session, row)
    if policy_error is not None:
        result = ProbeResult(
            status="invalid", http_status=None, error=policy_error,
        )
    else:
        api_key = await _get_provider_api_key(session, row)
        result = await probe_connection(
            row.provider_type, row.base_url, api_key,
        )

    now = datetime.now(UTC)
    await session.execute(
        update(ProviderConnection)
        .where(ProviderConnection.id == row.id)
        .values(
            status=result.status,
            last_validated_at=now,
            last_validation_error=result.error,
        ),
    )
    await write_admin_audit_event(
        session,
        actor=_provider_audit_actor(ctx, x_loom_admin_actor),
        action="provider_connection.test",
        target_type="provider_connection",
        target_id=str(row.id),
        request=request,
        metadata=_provider_audit_metadata(
            row,
            extra={
                "status": result.status,
                "http_status": result.http_status,
            },
        ),
    )
    await session.commit()
    return ProviderConnectionTestResponse(
        connection_id=row.id,
        status=result.status,
        http_status=result.http_status,
        last_validation_error=result.error,
        last_validated_at=now,
    )


@router.get(
    "/provider-connections/{connection_id}/models",
    response_model=ProviderModelCacheListResponse,
)
async def list_models(
    connection_id: UUID, sc: SessionAndCtx,
) -> ProviderModelCacheListResponse:
    """Return every cached model row for the connection.

    No auto-refresh: the cache is populated on `POST /models/refresh`
    only. The CLI surfaces an empty list with a hint to run refresh.
    Ordering by model_id keeps the output stable for CI use.
    """
    session, ctx = sc
    row = await _get_active_connection(session, connection_id, ctx)
    rows = (await session.execute(
        select(ProviderModelCache)
        .where(ProviderModelCache.provider_connection_id == row.id)
        .order_by(ProviderModelCache.model_id.asc()),
    )).scalars().all()
    return ProviderModelCacheListResponse(
        items=[_cache_row_to_entry(r) for r in rows],
    )


@router.post(
    "/provider-connections/{connection_id}/models",
    response_model=ProviderModelCacheEntry,
    status_code=201,
)
async def create_manual_model(
    connection_id: UUID,
    payload: ProviderModelManualCreate,
    sc: SessionAndCtx,
) -> ProviderModelCacheEntry:
    """Upsert an explicit model id for endpoints with no discovery.

    Manual rows share `provider_models_cache` so the selector sees one
    per-connection catalog. They remain visible even when a later
    upstream refresh does not return the id.
    """
    session, ctx = sc
    _require_provider_management(ctx)
    row = await _get_active_connection(session, connection_id, ctx)
    _require_provider_owner_or_admin(ctx, row)
    now = datetime.now(UTC)
    capabilities = dict(payload.capabilities)
    capabilities["source"] = "manual"
    stmt = pg_insert(ProviderModelCache).values(
        provider_connection_id=row.id,
        model_id=payload.model_id,
        family=payload.family,
        context_length=payload.context_length,
        capabilities=capabilities,
        visible=True,
        hidden_reason=None,
        last_seen_at=now,
        upstream_present=False,
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[
            ProviderModelCache.provider_connection_id,
            ProviderModelCache.model_id,
        ],
        set_=dict(
            family=payload.family,
            context_length=payload.context_length,
            capabilities=capabilities,
            visible=True,
            hidden_reason=None,
            last_seen_at=now,
            upstream_present=False,
        ),
    )
    await session.execute(stmt)
    await session.commit()
    cache_row = (await session.execute(
        select(ProviderModelCache).where(
            ProviderModelCache.provider_connection_id == row.id,
            ProviderModelCache.model_id == payload.model_id,
        ),
    )).scalar_one()
    return _cache_row_to_entry(cache_row)


@router.post(
    "/provider-connections/{connection_id}/models/refresh",
    response_model=ProviderModelRefreshResponse,
)
async def refresh_models(
    connection_id: UUID, sc: SessionAndCtx,
) -> ProviderModelRefreshResponse:
    """Probe the upstream `/models` endpoint and upsert the cache.

    For each id returned by the upstream:
    - existing row → bump `last_seen_at`, set `upstream_present=true`;
      if the row was marked `missing-upstream` (system-hidden), flip
      it back to visible (operator-hidden rows stay hidden — refresh
      MUST NOT clobber operator overrides).
    - new row → INSERT visible=true, upstream_present=true.

    For each existing row NOT returned by the upstream:
    - set `upstream_present=false` and (only if not operator-hidden)
      `visible=false, hidden_reason='missing-upstream'`. Operator-
      hidden rows keep their `hidden_reason='operator-hidden'`.

    Upstream errors (network / non-2xx / parse) bubble up as 502;
    the cache is left untouched (no partial commits).
    """
    session, ctx = sc
    _require_provider_management(ctx)
    row = await _get_active_connection(session, connection_id, ctx)
    _require_provider_owner_or_admin(ctx, row)

    policy_error = await _refresh_current_egress_or_error(session, row)
    if policy_error is not None:
        raise HTTPException(status_code=400, detail=policy_error)

    api_key = await _get_provider_api_key(session, row)

    try:
        upstream_ids = await fetch_upstream_models(
            row.provider_type, row.base_url, api_key,
        )
    except UpstreamModelFetchError as e:
        # 502 — operator's upstream is the source of the failure, not
        # loom_service itself. Structured detail so the SPA can show a
        # typed message (#918) without embedding upstream HTML bodies.
        detail: dict[str, object] = {
            "code": e.error_code,
            "message": str(e),
        }
        if e.http_status is not None:
            detail["upstream_http_status"] = e.http_status
        raise HTTPException(status_code=502, detail=detail) from e

    upstream_set = set(upstream_ids)
    existing_rows = (await session.execute(
        select(ProviderModelCache).where(
            ProviderModelCache.provider_connection_id == row.id,
        ),
    )).scalars().all()
    existing_by_id = {r.model_id: r for r in existing_rows}
    existing_ids = set(existing_by_id.keys())

    now = datetime.now(UTC)

    # Upsert each upstream id. INSERT ... ON CONFLICT DO UPDATE handles
    # the new vs. seen split atomically; we count after-the-fact.
    added = 0
    refreshed = 0
    for mid in upstream_ids:
        existing = existing_by_id.get(mid)
        if existing is None:
            added += 1
        else:
            refreshed += 1
        # Determine visibility post-refresh:
        # - operator-hidden stays hidden
        # - missing-upstream → unhide (we just saw it)
        # - everything else → visible
        if existing is not None and existing.hidden_reason == "operator-hidden":
            visible = False
            hidden_reason: str | None = "operator-hidden"
        else:
            visible = True
            hidden_reason = None

        stmt = pg_insert(ProviderModelCache).values(
            provider_connection_id=row.id,
            model_id=mid,
            last_seen_at=now,
            upstream_present=True,
            visible=visible,
            hidden_reason=hidden_reason,
            capabilities={"source": "discovered"},
        )
        update_values: dict[str, object] = dict(
            last_seen_at=now,
            upstream_present=True,
            visible=visible,
            hidden_reason=hidden_reason,
        )
        if existing is None or not _is_manual_model(existing):
            update_values["capabilities"] = {"source": "discovered"}
        stmt = stmt.on_conflict_do_update(
            index_elements=[
                ProviderModelCache.provider_connection_id,
                ProviderModelCache.model_id,
            ],
            set_=update_values,
        )
        await session.execute(stmt)

    # Mark anything we DIDN'T see as missing. Operator-hidden rows keep
    # their hidden_reason; only flip upstream_present.
    missing_ids = existing_ids - upstream_set
    missing = len(missing_ids)
    for mid in missing_ids:
        prev = existing_by_id[mid]
        if prev.hidden_reason == "operator-hidden":
            await session.execute(
                update(ProviderModelCache)
                .where(
                    ProviderModelCache.provider_connection_id == row.id,
                    ProviderModelCache.model_id == mid,
                )
                .values(upstream_present=False),
            )
        elif _is_manual_model(prev):
            await session.execute(
                update(ProviderModelCache)
                .where(
                    ProviderModelCache.provider_connection_id == row.id,
                    ProviderModelCache.model_id == mid,
                )
                .values(
                    upstream_present=False,
                    visible=True,
                    hidden_reason=None,
                ),
            )
        else:
            await session.execute(
                update(ProviderModelCache)
                .where(
                    ProviderModelCache.provider_connection_id == row.id,
                    ProviderModelCache.model_id == mid,
                )
                .values(
                    upstream_present=False,
                    visible=False,
                    hidden_reason="missing-upstream",
                ),
            )

    await session.commit()

    refreshed_rows = (await session.execute(
        select(ProviderModelCache)
        .where(ProviderModelCache.provider_connection_id == row.id)
        .order_by(ProviderModelCache.model_id.asc()),
    )).scalars().all()
    return ProviderModelRefreshResponse(
        added=added,
        refreshed=refreshed,
        missing=missing,
        items=[_cache_row_to_entry(r) for r in refreshed_rows],
    )


@router.patch(
    "/provider-connections/{connection_id}/models/{model_id:path}/metadata",
    response_model=ProviderModelCacheEntry,
)
async def update_cached_model_metadata(
    request: Request,
    connection_id: UUID,
    model_id: str,
    payload: ProviderModelMetadataUpdate,
    sc: SessionAndCtx,
    x_loom_admin_actor: str | None = Header(default=None),
) -> ProviderModelCacheEntry:
    """Declare a verified capability without changing discovery or entitlement.

    The specification URL is retained as operator evidence, never fetched or
    interpreted as paid preflight success. Existing discovery and hide state
    are preserved; this cannot insert a model that has not been cached.
    """
    session, ctx = sc
    _require_provider_management(ctx)
    row = await _get_active_connection(session, connection_id, ctx)
    _require_provider_owner_or_admin(ctx, row)
    cache = await _get_cached_model_or_404(session, row.id, model_id)
    cache.context_length = payload.context_length
    cache.capabilities = {
        **(cache.capabilities or {}),
        "context_specification_url": payload.specification_url,
    }
    await write_admin_audit_event(
        session,
        actor=_provider_audit_actor(ctx, x_loom_admin_actor),
        action="provider_model.metadata",
        target_type="provider_model",
        target_id=f"{row.id}:{model_id}",
        request=request,
        metadata={
            "context_length": payload.context_length,
            "specification_url": payload.specification_url,
        },
    )
    await session.commit()
    return _cache_row_to_entry(cache)


@router.post(
    "/provider-connections/{connection_id}/models/{model_id:path}/preflight",
    response_model=ProviderModelCacheEntry,
)
async def preflight_cached_model(
    request: Request,
    connection_id: UUID,
    model_id: str,
    sc: SessionAndCtx,
    x_loom_admin_actor: str | None = Header(default=None),
) -> ProviderModelCacheEntry:
    """Run a bounded entitlement check for one cached model id.

    Discovery only proves that an upstream advertises a model. This route
    sends one minimal generation request so operators can see whether the
    current connection/key is allowed to call that exact model.
    """
    session, ctx = sc
    _require_provider_management(ctx)
    row = await _get_active_connection(session, connection_id, ctx)
    _require_provider_owner_or_admin(ctx, row)
    cache_row = await _get_cached_model_or_404(session, row.id, model_id)

    policy_error = await _refresh_current_egress_or_error(session, row)
    if policy_error is not None:
        result = ModelPreflightResult(
            status="failed",
            http_status=None,
            error_code="egress-policy-rejected",
            error_message=policy_error,
        )
    else:
        api_key = await _get_provider_api_key(session, row)
        result = await preflight_model(
            row.provider_type, row.base_url, api_key, model_id,
        )

    now = datetime.now(UTC)
    cache_row.last_preflight_status = result.status
    cache_row.last_preflight_at = now
    cache_row.last_preflight_http_status = result.http_status
    cache_row.last_preflight_error_code = result.error_code
    cache_row.last_preflight_error_message = result.error_message
    await session.flush()
    await write_admin_audit_event(
        session,
        actor=_provider_audit_actor(ctx, x_loom_admin_actor),
        action="provider_model.preflight",
        target_type="provider_model",
        target_id=f"{row.id}:{model_id}",
        request=request,
        metadata=_provider_audit_metadata(
            row,
            extra={
                "model_id": model_id,
                "status": result.status,
                "http_status": result.http_status,
                "error_code": result.error_code,
            },
        ),
    )
    await session.commit()
    return _cache_row_to_entry(cache_row)


@router.post(
    "/provider-connections/{connection_id}/models/{model_id:path}/hide",
    response_model=ProviderModelCacheEntry,
)
async def hide_model(
    connection_id: UUID, model_id: str, sc: SessionAndCtx,
) -> ProviderModelCacheEntry:
    """Mark a cached model `visible=false` with reason `operator-hidden`.
    Subsequent refreshes will not flip it back to visible — the operator's
    hide overrides upstream presence.

    404 if the model isn't in the cache for this connection. The CLI
    hints to run /refresh first when this happens.
    """
    session, ctx = sc
    _require_provider_management(ctx)
    row = await _get_active_connection(session, connection_id, ctx)
    _require_provider_owner_or_admin(ctx, row)
    cache_row = await _get_cached_model_or_404(session, row.id, model_id)
    cache_row.visible = False
    cache_row.hidden_reason = "operator-hidden"
    await session.commit()
    return _cache_row_to_entry(cache_row)


@router.post(
    "/provider-connections/{connection_id}/models/{model_id:path}/unhide",
    response_model=ProviderModelCacheEntry,
)
async def unhide_model(
    connection_id: UUID, model_id: str, sc: SessionAndCtx,
) -> ProviderModelCacheEntry:
    """Reverse a previous hide. If the model is still upstream-present,
    sets `visible=true, hidden_reason=NULL`. If the model is no longer
    upstream-present, sets `visible=false, hidden_reason='missing-upstream'`
    — operators can't make a missing model visible without re-refreshing
    and finding it. Manual models are the exception: they are explicitly
    operator-provided catalog rows for providers without upstream discovery,
    so they can be made visible even when `upstream_present=false`.

    404 if the model isn't in the cache.
    """
    session, ctx = sc
    _require_provider_management(ctx)
    row = await _get_active_connection(session, connection_id, ctx)
    _require_provider_owner_or_admin(ctx, row)
    cache_row = await _get_cached_model_or_404(session, row.id, model_id)
    if cache_row.upstream_present or _is_manual_model(cache_row):
        cache_row.visible = True
        cache_row.hidden_reason = None
    else:
        cache_row.visible = False
        cache_row.hidden_reason = "missing-upstream"
    await session.commit()
    return _cache_row_to_entry(cache_row)


@router.delete(
    "/provider-connections/{connection_id}",
    status_code=204,
)
async def delete_connection(
    request: Request,
    connection_id: UUID,
    sc: SessionAndCtx,
    x_loom_admin_actor: str | None = Header(default=None),
) -> None:
    """Soft-delete: sets `deleted_at = now()`. Existing Trial /
    Batch records retain attribution for billing/audit. Re-using the
    display_name is permitted after
    soft-delete (partial UNIQUE WHERE deleted_at IS NULL).

    Retire the key atomically; the collector preserves it for a full grace
    period and while another active or historical consumer references it.
    """
    session, ctx = sc
    _require_provider_management(ctx)
    row = await _get_active_connection(session, connection_id, ctx, for_update=True)
    _require_provider_owner_or_admin(ctx, row)
    await retire_provider_secret(session, ref=row.encrypted_api_key_ref, team_id=row.team_id)
    await session.execute(
        update(ProviderConnection)
        .where(ProviderConnection.id == row.id)
        .values(deleted_at=datetime.now(UTC), status="disabled"),
    )
    await write_admin_audit_event(
        session,
        actor=_provider_audit_actor(ctx, x_loom_admin_actor),
        action="provider_connection.delete",
        target_type="provider_connection",
        target_id=str(row.id),
        request=request,
        metadata=_provider_audit_metadata(row),
    )
    await session.commit()
