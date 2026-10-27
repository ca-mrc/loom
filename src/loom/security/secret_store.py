"""SecretStore Protocol + the local-encrypted Postgres-backed impl.

The Protocol lets gateway code use opaque secret references. The current
implementation stores AES-GCM-encrypted Postgres rows addressed by
``loom://...`` refs; other schemes are rejected.

Why async: the gateway is FastAPI; sync DB calls would block the event
loop. psycopg via SQLAlchemy 2.0's async session is the existing
convention (see ``loom_service/dependencies.py``).

Why AES-GCM: authenticated encryption with associated data. Tampering
with the ciphertext, the nonce, or the master-key version produces
``InvalidTag`` at decrypt time rather than silently returning bad bytes.

Master key: 32 bytes (AES-256). Loaded once at construction from the
operator-supplied ``LOOM_SECRET_STORE_MASTER_KEY`` (singular) or
``LOOM_SECRET_STORE_MASTER_KEYS`` (plural, comma-separated) env var,
base64-encoded.

Multi-key online rotation (approach C):
  ``LOOM_SECRET_STORE_MASTER_KEYS`` accepts a comma-separated list of
  base64-encoded 32-byte keys. The FIRST entry is the PRIMARY key used
  for new encrypts (``put``) and as the rewrap target. Subsequent entries
  are FALLBACK keys: ``get`` tries the fallback if the row's
  ``master_key_version`` doesn't match the primary version. This allows
  zero-downtime master-key rotation:

  Step 1: deploy new-as-primary + old-as-fallback
  Step 2: run ``loom admin secret-store rewrap --new-key <NEW>``
  Step 3: deploy new-only (drop fallback)

  ``master_key_version`` identifies which generation encrypted each row
  so the rotation walker can decrypt with the historic key and
  re-encrypt with the primary. For the primary key, version = the
  maximum version found across fallback keys + 1; for backward
  compatibility with existing single-key deployments, version = 1.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Protocol, runtime_checkable
from uuid import UUID, uuid4

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import Secret


class SecretStoreError(Exception):
    """Base class for SecretStore errors."""


class SecretNotFoundError(SecretStoreError):
    """get(ref) was called on a ref that doesn't exist (or was deleted)."""


class InvalidRefError(SecretStoreError):
    """ref doesn't parse as a valid loom://<namespace>/<uuid> URL."""


class DecryptError(SecretStoreError):
    """Ciphertext failed AEAD verification — wrong key, tampered ciphertext,
    or wrong master_key_version with no prev-key configured."""


@dataclass(frozen=True)
class ParsedRef:
    """A ``loom://<namespace>/<uuid>`` ref decomposed into its parts.

    Refs are intentionally opaque to callers (just strings). This
    dataclass is exposed for the rotation walker + admin tools that
    need to list / filter by namespace; ordinary callers should treat
    refs as cookies and pass them around unmodified.
    """
    scheme: str
    namespace: str
    secret_id: UUID

    def as_string(self) -> str:
        return f"{self.scheme}://{self.namespace}/{self.secret_id}"


def parse_ref(ref: str) -> ParsedRef:
    """Parse a ``loom://<namespace>/<uuid>`` ref. Validates the scheme,
    namespace presence, and UUID format. Raises :class:`InvalidRefError`
    on any malformed input — callers should treat this as a 400 if it
    came from user-supplied data, or as a 500 if it came from a row
    we wrote (DB corruption).
    """
    if "://" not in ref:
        raise InvalidRefError(f"ref missing scheme separator: {ref!r}")
    scheme, rest = ref.split("://", 1)
    if scheme != "loom":
        raise InvalidRefError(
            f"unsupported ref scheme {scheme!r}; expected 'loom' "
            f"(other schemes route to a different SecretStore impl)",
        )
    if "/" not in rest:
        raise InvalidRefError(
            f"ref missing namespace/uuid separator: {ref!r}",
        )
    namespace, uuid_str = rest.rsplit("/", 1)
    if not namespace:
        raise InvalidRefError(f"ref namespace is empty: {ref!r}")
    try:
        secret_id = UUID(uuid_str)
    except ValueError as e:
        raise InvalidRefError(f"ref uuid does not parse: {ref!r}") from e
    return ParsedRef(scheme=scheme, namespace=namespace, secret_id=secret_id)


@runtime_checkable
class SecretStore(Protocol):
    """Async Protocol for encrypted-secret storage.

    Current implementation:

    - :class:`LocalEncryptedSecretStore` — AES-GCM ciphertext in the
      ``secrets`` Postgres table. Default in both ``loom service`` and
      ``loom cluster`` for the user-API-key data path.
    """

    async def put(self, *, namespace: str, value: str) -> str:
        """Encrypt + persist ``value``; return the opaque ref the caller
        stores. Idempotency: every call generates a fresh ref (new UUID
        + new nonce), so two puts of the same value yield different refs.
        Namespacing is for query/audit grouping (typically the team_id
        as a string); it doesn't affect encryption.
        """
        ...

    async def get(self, ref: str) -> str:
        """Decrypt + return the secret value. Raises
        :class:`SecretNotFoundError` if ref is unknown,
        :class:`DecryptError` if the ciphertext fails AEAD verification.
        """
        ...

    async def delete(self, ref: str) -> None:
        """Remove the secret. Idempotent — deleting an unknown ref is
        a no-op (no exception). Rationale: simplifies caller code in
        rollback paths (``DELETE provider_connection`` → ``DELETE
        secret`` doesn't care if the secret was never created).
        """
        ...

    def list_refs(
        self, *, namespace: str | None = None,
    ) -> AsyncIterator[str]:
        """Yield every ref the store knows about, optionally filtered
        by namespace. For the rotation walker + admin audit tools.
        Implementations stream rather than materialize so an
        unbounded secrets table doesn't blow memory.
        """
        ...

    async def rewrap(self, ref: str, *, new_master_key: bytes) -> str:
        """Decrypt with the current master key, re-encrypt with the
        supplied new master key, persist. Returns the ref — for
        ``local-encrypted`` the ref is stable (only ciphertext +
        master_key_version change).
        """
        ...


_MASTER_KEY_LEN = 32  # AES-256
_NONCE_LEN = 12       # AES-GCM standard


def _decode_b64_key(raw: str, label: str) -> bytes:
    """Decode a base64-encoded 32-byte key; raise SecretStoreError on bad input."""
    import base64

    try:
        key = base64.b64decode(raw.strip(), validate=True)
    except (ValueError, base64.binascii.Error) as e:  # type: ignore[attr-defined]
        raise SecretStoreError(
            f"{label} is not valid base64: {e}",
        ) from e
    if len(key) != _MASTER_KEY_LEN:
        raise SecretStoreError(
            f"{label} decodes to {len(key)} bytes; expected "
            f"{_MASTER_KEY_LEN} (AES-256)",
        )
    return key


def load_master_keys_from_env() -> tuple[bytes, int, dict[int, bytes]]:
    """Load master key(s) from env vars.

    Resolution order:

    1. ``LOOM_SECRET_STORE_MASTER_KEYS`` (plural, comma-separated base64
       keys). First key = PRIMARY; remaining keys = FALLBACKS. Versions
       are assigned as (len(keys), len(keys)-1, ..., 1) so the primary
       has the highest version number and each fallback is one step older.
       This ensures the version bumps monotonically on each rotation.

    2. ``LOOM_SECRET_STORE_MASTER_KEY`` (singular, backward compat). Used
       as the sole key at version 1.

    Both env vars set simultaneously → fail-fast (ambiguous config).

    Returns:
        (primary_key, primary_version, {version: fallback_key, ...})
    """
    singular = os.environ.get("LOOM_SECRET_STORE_MASTER_KEY", "")
    plural = os.environ.get("LOOM_SECRET_STORE_MASTER_KEYS", "")

    if singular and plural:
        raise SecretStoreError(
            "Both LOOM_SECRET_STORE_MASTER_KEY and "
            "LOOM_SECRET_STORE_MASTER_KEYS are set. Use only one. "
            "During key rotation use LOOM_SECRET_STORE_MASTER_KEYS "
            "(plural) with a comma-separated list: NEW,OLD."
        )

    if not singular and not plural:
        raise SecretStoreError(
            "Neither LOOM_SECRET_STORE_MASTER_KEY nor "
            "LOOM_SECRET_STORE_MASTER_KEYS is set. "
            "Generate a key with: python -c 'import os, base64; "
            "print(base64.b64encode(os.urandom(32)).decode())'",
        )

    if singular:
        key = _decode_b64_key(singular, "LOOM_SECRET_STORE_MASTER_KEY")
        return key, 1, {}

    # plural path: comma-separated list
    parts = [p.strip() for p in plural.split(",") if p.strip()]
    if not parts:
        raise SecretStoreError(
            "LOOM_SECRET_STORE_MASTER_KEYS is set but contains no keys",
        )
    total = len(parts)
    # Primary gets the highest version; fallbacks get descending versions.
    primary_version = total
    primary_key = _decode_b64_key(
        parts[0], "LOOM_SECRET_STORE_MASTER_KEYS[0] (primary)",
    )
    fallbacks: dict[int, bytes] = {}
    for i, part in enumerate(parts[1:], start=1):
        version = total - i  # primary=N, 1st fallback=N-1, ...
        fallbacks[version] = _decode_b64_key(
            part, f"LOOM_SECRET_STORE_MASTER_KEYS[{i}] (fallback v{version})",
        )
    return primary_key, primary_version, fallbacks


class LocalEncryptedSecretStore:
    """Postgres-backed AES-GCM SecretStore.

    Refs have the format ``loom://<namespace>/<uuid>``. The UUID is
    generated per-put; the namespace is operator-supplied (typically
    ``team:<team_id>`` for user API keys).

    ``master_key_version`` lets the rotation walker identify which key
    generation encrypted each row. This impl supports multi-key online
    rotation via ``LOOM_SECRET_STORE_MASTER_KEYS`` (see module docstring).

    The ``fallback_keys`` dict maps ``{version: key_bytes}`` for keys
    that were previously primary but are now being phased out. ``get()``
    tries the primary key first; if the row's version matches a fallback,
    it decrypts with that fallback instead.

    Thread/coroutine safety: stateless apart from the loaded master keys.
    Safe to share one instance across the FastAPI worker process.
    """

    def __init__(
        self,
        session: AsyncSession,
        *,
        master_key: bytes | None = None,
        master_key_version: int = 1,
        fallback_keys: dict[int, bytes] | None = None,
    ) -> None:
        if master_key is None:
            master_key, master_key_version, loaded_fallbacks = (
                load_master_keys_from_env()
            )
            if fallback_keys is None:
                fallback_keys = loaded_fallbacks
        if len(master_key) != _MASTER_KEY_LEN:
            raise SecretStoreError(
                f"master_key length {len(master_key)} != {_MASTER_KEY_LEN}",
            )
        self._session = session
        self._primary_key = master_key
        self._aead = AESGCM(master_key)
        self._master_key_version = master_key_version
        # Build fallback AEAD map: {version: AESGCM}
        self._fallback_aeads: dict[int, AESGCM] = {}
        for ver, fb_key in (fallback_keys or {}).items():
            if len(fb_key) != _MASTER_KEY_LEN:
                raise SecretStoreError(
                    f"fallback_key v{ver} length {len(fb_key)} != "
                    f"{_MASTER_KEY_LEN}",
                )
            self._fallback_aeads[ver] = AESGCM(fb_key)

    def _aead_for_version(self, version: int) -> AESGCM:
        """Return the AEAD for the given master_key_version.

        Primary version → primary AEAD.
        Known fallback version → fallback AEAD.
        Unknown version → DecryptError (never silently use wrong key).
        """
        if version == self._master_key_version:
            return self._aead
        if version in self._fallback_aeads:
            return self._fallback_aeads[version]
        known = sorted([self._master_key_version, *self._fallback_aeads])
        raise DecryptError(
            f"secret row uses master_key_version={version}, but the "
            f"store only knows versions {known}. Either load the "
            f"corresponding key in LOOM_SECRET_STORE_MASTER_KEYS or "
            f"run the rewrap walker to migrate to the current key.",
        )

    async def put(self, *, namespace: str, value: str) -> str:
        if not namespace:
            raise InvalidRefError("namespace must be non-empty")
        secret_id = uuid4()
        ref = f"loom://{namespace}/{secret_id}"
        nonce = os.urandom(_NONCE_LEN)
        # Bind the ref as Additional Authenticated Data (AAD) so a row
        # whose ciphertext was copy-pasted into a different ref slot
        # fails decryption. Defense against a future bug where ref-vs-row
        # gets shuffled.
        ciphertext = self._aead.encrypt(
            nonce, value.encode("utf-8"), associated_data=ref.encode("utf-8"),
        )
        secret = Secret(
            ref=ref,
            ciphertext=ciphertext,
            nonce=nonce,
            master_key_version=self._master_key_version,
        )
        self._session.add(secret)
        await self._session.flush()
        return ref

    async def get(self, ref: str) -> str:
        parse_ref(ref)  # validates format; raises InvalidRefError
        row = (await self._session.execute(
            select(Secret).where(Secret.ref == ref),
        )).scalar_one_or_none()
        if row is None:
            raise SecretNotFoundError(f"no secret for ref {ref!r}")
        aead = self._aead_for_version(row.master_key_version)
        try:
            plaintext = aead.decrypt(
                row.nonce, row.ciphertext,
                associated_data=ref.encode("utf-8"),
            )
        except InvalidTag as e:
            raise DecryptError(
                f"AEAD verification failed for {ref!r}; ciphertext, "
                f"nonce, or master key has changed",
            ) from e
        return plaintext.decode("utf-8")

    async def delete(self, ref: str) -> None:
        # Validate format (catches typos at the API boundary) but
        # treat row-not-found as success — delete-on-missing is the
        # standard idempotent contract.
        parse_ref(ref)
        await self._session.execute(
            delete(Secret).where(Secret.ref == ref),
        )
        await self._session.flush()

    async def list_refs(
        self, *, namespace: str | None = None,
    ) -> AsyncIterator[str]:
        stmt = select(Secret.ref)
        if namespace is not None:
            # ref format is `loom://<namespace>/<uuid>`, so the namespace
            # filter is a prefix match on `loom://<namespace>/`.
            prefix = f"loom://{namespace}/"
            stmt = stmt.where(Secret.ref.startswith(prefix))
        result = await self._session.stream(stmt)
        async for row in result:
            yield row[0]

    async def rewrap(self, ref: str, *, new_master_key: bytes) -> str:
        """Re-encrypt the secret with ``new_master_key``.

        The ref is stable (returned unchanged); the row's ciphertext +
        nonce + master_key_version are updated in place.

        The new ``master_key_version`` is ``self._master_key_version + 1``
        when ``new_master_key != self._primary_key``, or stays at
        ``self._master_key_version`` when new_master_key IS the primary key
        (idempotent rewrap to same key — use during the bulk-walk pass when
        the key is already the primary in ``LOOM_SECRET_STORE_MASTER_KEYS``).

        The rewrap endpoint (``POST /api/v1/admin/secret-store/rewrap``)
        always calls this with ``new_master_key == self._primary_key`` so
        that rows from fallback versions are migrated to the primary.
        """
        if len(new_master_key) != _MASTER_KEY_LEN:
            raise SecretStoreError(
                f"new_master_key length {len(new_master_key)} != "
                f"{_MASTER_KEY_LEN}",
            )
        # Lock before decrypting so rewrap and collection cannot act on stale rows.
        # populate_existing also refreshes any row already in this session.
        await self._session.execute(
            select(Secret).where(Secret.ref == ref).with_for_update()
            .execution_options(populate_existing=True),
        )
        plaintext = await self.get(ref)
        new_aead = AESGCM(new_master_key)
        new_nonce = os.urandom(_NONCE_LEN)
        new_ciphertext = new_aead.encrypt(
            new_nonce, plaintext.encode("utf-8"),
            associated_data=ref.encode("utf-8"),
        )
        # In-place UPDATE preserves the ref string the consuming row
        # (e.g., provider_connections.encrypted_api_key_ref) already holds.
        row = (await self._session.execute(
            select(Secret).where(Secret.ref == ref),
        )).scalar_one()
        row.ciphertext = new_ciphertext
        row.nonce = new_nonce
        # If new key == primary key, stamp with the primary version.
        # Otherwise bump to primary_version+1 (caller supplies a truly
        # NEW key, not yet the primary — rare outside testing).
        if new_master_key == self._primary_key:
            row.master_key_version = self._master_key_version
        else:
            row.master_key_version = self._master_key_version + 1
        await self._session.flush()
        return ref


async def assert_existing_secrets_decryptable(session: AsyncSession) -> int:
    """Fail fast if existing local-encrypted secrets do not decrypt.

    The service and gateway normally discover a bad
    ``LOOM_SECRET_STORE_MASTER_KEY`` only when a provider request tries to
    decrypt ``provider_connections.encrypted_api_key_ref``. Running this once
    during process startup turns that latent HTTP 500 into an operator-visible
    startup failure.

    Empty stores are valid and do not require a configured master key. This
    keeps fresh databases and tests that never created provider connections
    from needing secret-store env vars unnecessarily.
    """
    result = await session.stream(select(Secret.ref))
    refs = [row[0] async for row in result]
    if not refs:
        return 0

    store = LocalEncryptedSecretStore(session)
    checked = 0
    for ref in refs:
        try:
            await store.get(ref)
        except SecretNotFoundError:
            # A concurrent retirement collector may remove a ref after the scan.
            continue
        except DecryptError as exc:
            raise DecryptError(
                "SecretStore startup validation failed for "
                f"{ref!r}; current LOOM_SECRET_STORE_MASTER_KEY/"
                "LOOM_SECRET_STORE_MASTER_KEYS cannot decrypt an existing "
                "secret. Restore the key that encrypted this row, configure it "
                "as a fallback in LOOM_SECRET_STORE_MASTER_KEYS, or run "
                "`loom admin secret-store rewrap` after loading the old key.",
            ) from exc
        checked += 1
    return checked
