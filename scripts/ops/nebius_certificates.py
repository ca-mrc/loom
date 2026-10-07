#!/usr/bin/env python3
"""Private certificate qualification for protected Nebius operations.

Generation publication is not a Kubernetes write or a public routing cutover.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import importlib.metadata
import json
import os
import re
import shlex
import ssl
import stat
import subprocess
import sys
import tempfile
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa
from cryptography.x509.oid import ExtendedKeyUsageOID
from cryptography.x509.verification import PolicyBuilder, Store, VerificationError

ROOT = Path(__file__).resolve().parents[2]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

_HOST = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+")
_GENERATION = re.compile(r"[0-9a-f]{64}")
_CERTIFICATE_PEM = rb"\s*(?:-----BEGIN CERTIFICATE-----\s+[A-Za-z0-9+/=\s]+-----END CERTIFICATE-----\s*)+"


class CertificateError(RuntimeError):
    """Fixed, payload-free certificate failure; never forward external errors."""


def certificate_names(child_domain: str, management_host: str) -> list[str]:
    if (any(len(name) > 250 or not _HOST.fullmatch(name) for name in (child_domain, management_host))
            or management_host == child_domain or management_host.endswith("." + child_domain)):
        raise CertificateError("invalid or overlapping certificate subjects")
    return ["*." + child_domain, management_host]


def validate_certificate(chain: bytes, key: bytes, *, child_domain: str, management_host: str,
                         now: datetime | None = None,
                         roots: Sequence[x509.Certificate] | None = None) -> dict[str, Any]:
    """Verify exact SANs, public trust, validity and possession before delivery.

    Tests can inject a local CA. Operational callers use the system public trust
    store, never a certificate-supplied root or a caller-selected CA file.
    """
    return _validate_certificate(chain, key, names=certificate_names(child_domain, management_host),
                                 now=now, roots=roots)


def validate_management_certificate(chain: bytes, key: bytes, *, management_host: str,
                                    now: datetime | None = None,
                                    roots: Sequence[x509.Certificate] | None = None) -> dict[str, Any]:
    """An independent manager receives exactly one host, never a shared wildcard."""
    return _validate_certificate(chain, key, names=_management_names(management_host), now=now, roots=roots)


def _management_names(host: str) -> list[str]:
    if not isinstance(host, str) or len(host) > 250 or not _HOST.fullmatch(host):
        raise CertificateError("invalid management certificate subject")
    return [host]


def _validate_certificate(chain: bytes, key: bytes, *, names: list[str], now: datetime | None,
                          roots: Sequence[x509.Certificate] | None) -> dict[str, Any]:
    if not 0 < len(chain) <= 65_536 or not 0 < len(key) <= 16_384:
        raise CertificateError("certificate material exceeds bounds")
    now = now or datetime.now(UTC)
    if now.tzinfo is None:
        raise CertificateError("certificate validation requires an aware clock")
    try:
        if not re.fullmatch(_CERTIFICATE_PEM, chain):
            raise CertificateError("invalid certificate encoding")
        certificates = x509.load_pem_x509_certificates(chain)
        if not 1 <= len(certificates) <= 5:
            raise CertificateError("invalid certificate chain length")
        leaf = certificates[0]
        private = serialization.load_pem_private_key(key, password=None)
        if not ((isinstance(private, rsa.RSAPrivateKey) and private.key_size >= 2048)
                or (isinstance(private, ec.EllipticCurvePrivateKey)
                    and isinstance(private.curve, (ec.SECP256R1, ec.SECP384R1)))):
            raise CertificateError("unsupported certificate key")
        if (leaf.public_key().public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)
                != private.public_key().public_bytes(serialization.Encoding.DER, serialization.PublicFormat.SubjectPublicKeyInfo)):
            raise CertificateError("certificate private key mismatch")
        sans = list(leaf.extensions.get_extension_for_class(x509.SubjectAlternativeName).value)
        if len(sans) != len(names) or set(sans) != {x509.DNSName(name) for name in names}:
            raise CertificateError("certificate subjects differ from protected scope")
        if leaf.extensions.get_extension_for_class(x509.BasicConstraints).value.ca:
            raise CertificateError("certificate leaf must not be a CA")
        if ExtendedKeyUsageOID.SERVER_AUTH not in leaf.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value:
            raise CertificateError("certificate does not permit server authentication")
        if leaf.not_valid_before_utc > now or leaf.not_valid_after_utc < now + timedelta(days=7):
            raise CertificateError("certificate is not valid for the delivery window")
        trusted = list(roots) if roots is not None else [
            x509.load_der_x509_certificate(der) for der in ssl.create_default_context().get_ca_certs(binary_form=True)
        ]
        (PolicyBuilder().store(Store(trusted)).time(now).build_server_verifier(x509.DNSName(names[-1]))
         .verify(leaf, certificates[1:]))
        return {"sans": names, "expires_at": leaf.not_valid_after_utc.isoformat(),
                "fingerprint_sha256": leaf.fingerprint(hashes.SHA256()).hex()}
    except CertificateError:
        raise
    except (ValueError, TypeError, x509.ExtensionNotFound, x509.DuplicateExtension, VerificationError):
        raise CertificateError("certificate validation failed") from None


def _sync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _private_directory(path: Path) -> None:
    # Trusted operator owns the parent. Reject symlinks in every path component,
    # not only the final component, before creating any private material.
    if path.absolute() != path.resolve():
        raise CertificateError("certificate state path must not contain symlinks")
    path.mkdir(mode=0o700, exist_ok=True)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise CertificateError("private certificate directory required")
    _sync_directory(path.parent)


def _private_read(path: Path, *, limit: int = 65_536) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or info.st_mode & 0o077 or info.st_nlink != 1):
            raise CertificateError("private regular certificate file required")
        value = stream.read(limit + 1)
    if len(value) > limit:
        raise CertificateError("private certificate file exceeds bound")
    return value


@contextmanager
def _locked_state(root: Path) -> Iterator[None]:
    try:
        _private_directory(root)
        descriptor = os.open(root / "operation.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        with os.fdopen(descriptor, "rb+") as stream:
            info = os.fstat(stream.fileno())
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or info.st_mode & 0o077 or info.st_nlink != 1):
                raise CertificateError("private certificate lock required")
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
            yield
    except OSError:
        raise CertificateError("certificate storage unavailable or already in use") from None


def _selected(root: Path) -> dict[str, Any] | None:
    path = root / "selected.json"
    try:
        path.lstat()
    except FileNotFoundError:
        return None
    try:
        value = json.loads(_private_read(path))
        if (not isinstance(value, dict) or value.get("schema") != "loom.nebius-certificate.v1"
                or set(value) != {"schema", "generation", "previous_generation", "sans", "expires_at", "fingerprint_sha256"}
                or not isinstance(value.get("generation"), str) or not _GENERATION.fullmatch(value["generation"])
                or (value["previous_generation"] is not None and (
                    not isinstance(value["previous_generation"], str) or not _GENERATION.fullmatch(value["previous_generation"])))
                or not isinstance(value.get("sans"), list)):
            raise CertificateError("invalid certificate selection")
        return value
    except (ValueError, RecursionError):
        raise CertificateError("invalid certificate selection") from None


def _write_private(path: Path, value: bytes) -> None:
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(value)
        stream.flush()
        os.fsync(stream.fileno())


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".selection-", delete=False) as stream:
        pending = Path(stream.name)
        stream.write(json.dumps(value, sort_keys=True).encode())
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(pending, path)
    _sync_directory(path.parent)


def _publish(root: Path, chain: bytes, key: bytes, report: dict[str, Any]) -> dict[str, Any]:
    previous = _selected(root)
    if previous is not None and previous["sans"] != report["sans"]:
        raise CertificateError("selected certificate belongs to different subjects")
    generations = root / "generations"
    _private_directory(generations)
    generation_id = hashlib.sha256(chain).hexdigest()
    generation = generations / generation_id
    if generation.exists() or generation.is_symlink():
        _private_directory(generation)
        if (_private_read(generation / "fullchain.pem") != chain
                or _private_read(generation / "privkey.pem", limit=16_384) != key):
            raise CertificateError("existing certificate generation differs")
    else:
        temporary = Path(tempfile.mkdtemp(prefix=".pending-", dir=generations))
        _write_private(temporary / "fullchain.pem", chain)
        _write_private(temporary / "privkey.pem", key)
        _sync_directory(temporary)
        os.rename(temporary, generation)
        _sync_directory(generations)
    if previous is not None and previous["generation"] == generation_id:
        if any(previous[key] != value for key, value in report.items()):
            raise CertificateError("persisted certificate metadata differs from validated generation")
        return previous
    selected = {"schema": "loom.nebius-certificate.v1", "generation": generation_id,
                "previous_generation": previous["generation"] if previous else None, **report}
    # Keep partially written generations for private recovery, but never select
    # them. Only the atomic manifest replacement changes the deliverable result.
    _atomic_json(root / "selected.json", selected)
    return selected


def publish_certificate(root: Path, chain: bytes, key: bytes, *, child_domain: str,
                        management_host: str, now: datetime | None = None,
                        roots: Sequence[x509.Certificate] | None = None) -> dict[str, Any]:
    report = validate_certificate(chain, key, child_domain=child_domain,
                                  management_host=management_host, now=now, roots=roots)
    with _locked_state(root):
        return _publish(root, chain, key, report)


def _installation_names(config: dict[str, Any]) -> list[str]:
    if config['schema'] == 'loom.nebius-management-certificate-installation.v1':
        return _management_names(config['management_host'])
    if config['schema'] != 'loom.nebius-certificate-installation.v1':
        raise CertificateError('invalid certificate subject scope')
    return certificate_names(config['child_domain'], config['management_host'])


def load_installation(path: Path, *, management_only: bool = False) -> dict[str, Any]:
    try:
        if not path.is_absolute() or path != path.resolve():
            raise CertificateError("protected certificate configuration must use an absolute private path")
        value = json.loads(_private_read(path, limit=16_384))
        fields = {"schema", "installation_id", "zone", "child_domain", "management_host", "credential_file", "state_dir", "email"}
        schema = 'loom.nebius-certificate-installation.v1'
        if management_only:
            fields.remove('child_domain')
            schema = 'loom.nebius-management-certificate-installation.v1'
        if (not isinstance(value, dict) or set(value) != fields
                or value["schema"] != schema
                or not all(isinstance(value[field], str) for field in fields - {"email"})
                or UUID(value["installation_id"]).int == 0):
            raise CertificateError("invalid protected certificate configuration")
        names = _installation_names(value)
        if not _HOST.fullmatch(value["zone"]) or not all(name.endswith("." + value["zone"]) for name in names):
            raise CertificateError("certificate subjects must belong to the protected DNS zone")
        if value["email"] is not None and (not isinstance(value["email"], str)
                or not re.fullmatch(r"[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]{1,64}@[A-Za-z0-9.-]{1,253}", value["email"])):
            raise CertificateError("invalid ACME contact")
        for field in ("state_dir", "credential_file"):
            candidate = Path(value[field])
            if not candidate.is_absolute() or candidate != candidate.resolve():
                raise CertificateError("certificate paths must be absolute and not traverse symlinks")
        return value
    except CertificateError:
        raise
    except (OSError, ValueError, TypeError, RecursionError):
        raise CertificateError("protected certificate configuration unavailable") from None


def _bind_installation(root: Path, config: dict[str, Any]) -> None:
    path = root / "installation.json"
    try:
        path.lstat()
    except FileNotFoundError:
        _atomic_json(path, config)
    else:
        if load_installation(path, management_only=config['schema'] == 'loom.nebius-management-certificate-installation.v1') != config:
            raise CertificateError("certificate state belongs to a different installation")


def _clean_challenges(root: Path, config: dict[str, Any]) -> None:
    challenges = root / "challenges"
    _private_directory(challenges)
    entries: list[Path] = []
    for entry in challenges.iterdir():
        if len(entries) >= 4096:
            raise CertificateError("certificate challenge journal exceeds bound")
        entries.append(entry)
    for path in entries:
        raw = _private_read(path, limit=262_144)
        if re.fullmatch(r"[0-9a-f]{64}\.lock", path.name) and not raw:
            continue
        if not re.fullmatch(r"[0-9a-f]{64}\.json", path.name):
            raise CertificateError("unknown challenge state requires reconciliation")
        try:
            value = json.loads(raw)
            if not isinstance(value, dict) or value.get("stage") != "deleted":
                raise CertificateError("unresolved DNS challenge requires reconciliation")
            identity = {key: value[key] for key in ("schema", "zone", "domain", "validation")}
            if (identity["schema"] != "loom.dns-challenge.v1" or identity["zone"] != config["zone"]
                    or identity["domain"] not in {name.removeprefix('*.') for name in _installation_names(config)}
                    or not isinstance(identity["validation"], str)
                    or not re.fullmatch(r"[A-Za-z0-9_-]{43}", identity["validation"])
                    or hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest() != path.stem):
                raise CertificateError("unknown challenge ownership requires reconciliation")
        except (ValueError, KeyError, TypeError, RecursionError):
            raise CertificateError("invalid challenge state requires reconciliation") from None


def _certbot_version() -> str:
    try:
        return importlib.metadata.version("certbot")
    except importlib.metadata.PackageNotFoundError:
        raise CertificateError("pinned certificate client unavailable") from None


def _run_client(args: list[str], *, timeout: int, start_new_session: bool,
                stdout: int, stderr: int) -> subprocess.CompletedProcess[bytes]:
    # No ambient proxy, Python module path or TLS-root override reaches Certbot
    # or its hooks. Logs are private files controlled by --logs-dir.
    from scripts.ops.nebius_certificate_gateway import run_private

    if not start_new_session or stdout != subprocess.DEVNULL or stderr != subprocess.DEVNULL:
        raise CertificateError("certificate client requires isolated private execution")
    run_private(args, timeout=timeout)
    return subprocess.CompletedProcess(args, 0)


def _lineage_material(root: Path) -> tuple[bytes, bytes]:
    archive = root / "acme" / "archive" / "loom-managed"
    if archive != archive.resolve():
        raise CertificateError("certificate archive must not traverse symlinks")
    result = []
    for name, limit in (("fullchain", 65_536), ("privkey", 16_384)):
        live = root / "acme" / "live" / "loom-managed" / (name + ".pem")
        resolved = live.resolve(strict=True)
        if resolved.parent != archive or not re.fullmatch(name + r"[1-9][0-9]*\.pem", resolved.name):
            raise CertificateError("certificate lineage escapes the owned archive")
        result.append(_private_read(resolved, limit=limit))
    return result[0], result[1]


def _audit_recovery(root: Path, *, sync: bool) -> None:
    """Reject unsafe persisted paths before client writes; sync recovery after.

    Certbot's normal live links are the only allowed symlinks. The gateway
    account is trusted; this is not a sandbox against hostile same-UID races.
    """
    files: list[Path] = []
    directories: list[Path] = []
    total = 0
    pending = [root / name for name in ("acme", "work", "logs")]
    while pending:
        path = pending.pop()
        if len(files) + len(directories) > 4096:
            raise CertificateError("certificate recovery tree exceeds entry bound")
        info = path.lstat()
        if info.st_uid != os.getuid():
            raise CertificateError("certificate recovery state has a foreign owner")
        if stat.S_ISLNK(info.st_mode):
            relative = path.relative_to(root)
            if not re.fullmatch(r"acme/live/loom-managed/(?:cert|chain|fullchain|privkey)\.pem", relative.as_posix()):
                raise CertificateError("certificate recovery state has an unexpected link")
            target = path.resolve(strict=True)
            if (target.parent != root / "acme" / "archive" / "loom-managed"
                    or not re.fullmatch(path.stem + r"[1-9][0-9]*\.pem", target.name)):
                raise CertificateError("certificate recovery link escapes its lineage")
            files.append(path)  # Count links but sync their regular target below.
        elif stat.S_ISDIR(info.st_mode):
            if info.st_mode & 0o077:
                raise CertificateError("certificate recovery directory is not private")
            directories.append(path)
            for child in path.iterdir():
                if len(pending) + len(files) + len(directories) >= 4096:
                    raise CertificateError("certificate recovery tree exceeds entry bound")
                pending.append(child)
        elif stat.S_ISREG(info.st_mode):
            if info.st_mode & 0o077 or info.st_nlink != 1 or info.st_size > 8 * 1024 * 1024:
                raise CertificateError("certificate recovery file exceeds private boundary")
            total += info.st_size
            if total > 32 * 1024 * 1024:
                raise CertificateError("certificate recovery data exceeds byte bound")
            files.append(path)
        else:
            raise CertificateError("certificate recovery state contains a special file")
    if not sync:
        return
    accounts: dict[str, set[str]] = {}
    for path in files:
        match = re.fullmatch(r"acme/accounts/acme-v02\.api\.letsencrypt\.org/directory/([0-9a-f]{32})/(private_key|regr|meta)\.json",
                             path.relative_to(root).as_posix())
        if match:
            accounts.setdefault(match[1], set()).add(match[2])
    if (len(accounts) != 1 or next(iter(accounts.values())) != {"private_key", "regr", "meta"}
            or root / "acme" / "renewal" / "loom-managed.conf" not in files):
        raise CertificateError("ACME account or renewal recovery state is incomplete")
    for path in files:
        if path.is_symlink():
            continue
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    for path in sorted(directories, key=lambda value: len(value.parts), reverse=True):
        _sync_directory(path)
    _sync_directory(root)


def issue_certificate(config_path: Path, *, now: datetime | None = None,
                      roots: Sequence[x509.Certificate] | None = None) -> dict[str, Any]:
    config = load_installation(config_path)
    return _issue_certificate(config, now=now, roots=roots)


def issue_management_certificate(config_path: Path, *, now: datetime | None = None,
                                  roots: Sequence[x509.Certificate] | None = None) -> dict[str, Any]:
    config = load_installation(config_path, management_only=True)
    return _issue_certificate(config, now=now, roots=roots)


def _issue_certificate(config: dict[str, Any], *, now: datetime | None,
                       roots: Sequence[x509.Certificate] | None) -> dict[str, Any]:
    root = Path(config["state_dir"])
    try:
        with _locked_state(root):
            _bind_installation(root, config)
            if _certbot_version() != "5.8.0":
                raise CertificateError("certificate client version differs from qualified pin")
            journal = root / "issuance.json"
            if journal.exists() or journal.is_symlink():
                value = json.loads(_private_read(journal))
                if (not isinstance(value, dict) or value.get("schema") != "loom.nebius-issuance.v1"
                        or value.get("stage") != "complete"):
                    raise CertificateError("unresolved issuance requires reconciliation; no automatic retry")
            _clean_challenges(root, config)
            for directory in ("acme", "work", "logs"):
                _private_directory(root / directory)
            _audit_recovery(root, sync=False)
            hook_action = ('management-hook' if config['schema'] == 'loom.nebius-management-certificate-installation.v1' else 'hook')
            hook = [sys.executable, str(Path(__file__).resolve()), hook_action]
            args = [sys.executable, "-c", "from certbot.main import main; raise SystemExit(main())",
                    "certonly", "--config", "/dev/null", "--non-interactive",
                    "--agree-tos", "--server", "https://acme-v02.api.letsencrypt.org/directory", "--manual",
                    "--preferred-challenges", "dns", "--cert-name", "loom-managed", "--keep-until-expiring",
                    "--no-directory-hooks", "--config-dir", str(root / "acme"), "--work-dir", str(root / "work"),
                    "--logs-dir", str(root / "logs"), "--max-log-backups", "3", "--key-type", "ecdsa",
                    "--manual-auth-hook", shlex.join([*hook, "auth", "--config", str(root / "installation.json")]),
                    "--manual-cleanup-hook", shlex.join([*hook, "cleanup", "--config", str(root / "installation.json")])]
            args += ["--email", config["email"]] if config["email"] else ["--register-unsafely-without-email"]
            for name in _installation_names(config):
                args += ["--domain", name]
            intent = {"schema": "loom.nebius-issuance.v1", "stage": "running", "started_at": (now or datetime.now(UTC)).isoformat()}
            _atomic_json(journal, intent)
            # A failed process or failed validation deliberately leaves running
            # intent. A later run must reconcile, never silently repeat a write.
            result = _run_client(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                 timeout=1800, start_new_session=True)
            if result.returncode:
                raise CertificateError("ACME client failed; preserve private logs and reconcile")
            _clean_challenges(root, config)
            _audit_recovery(root, sync=True)
            chain, key = _lineage_material(root)
            report = _validate_certificate(chain, key, names=_installation_names(config), now=now, roots=roots)
            selected = _publish(root, chain, key, report)
            _atomic_json(journal, {**intent, "stage": "complete", "generation": selected["generation"]})
            return {"status": "qualified", "installation_id": config["installation_id"], **selected}
    except CertificateError:
        raise
    except Exception:
        raise CertificateError("certificate issuance failed; preserve private state and reconcile") from None


def certificate_hook(config_path: Path, action: str) -> str:
    config = load_installation(config_path)
    return _certificate_hook(config, action)


def management_certificate_hook(config_path: Path, action: str) -> str:
    config = load_installation(config_path, management_only=True)
    return _certificate_hook(config, action)


def _certificate_hook(config: dict[str, Any], action: str) -> str:
    domain = os.environ.get("CERTBOT_DOMAIN", "")
    names = _installation_names(config)
    allowed = {*names, *(name.removeprefix('*.') for name in names)}
    if domain not in allowed or action not in {"auth", "cleanup"}:
        raise CertificateError("certificate hook is outside the protected subject allowlist")
    # Imported only after scope checks; this module is an operations dependency,
    # not a provider SDK available to application-mode code.
    from scripts.ops.nebius_dns_challenge import GoDaddyDNS, load_token, run_hook, wait_for_txt

    with GoDaddyDNS(config["zone"], domain.removeprefix("*."), load_token(Path(config["credential_file"]))) as provider:
        return run_hook(provider, state_dir=Path(config["state_dir"]) / "challenges", action=action,
                        certbot_domain=domain, validation=os.environ.get("CERTBOT_VALIDATION", ""), wait=wait_for_txt)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="operation", required=True)
    for name in ('issue', 'issue-management'):
        issue = sub.add_parser(name)
        issue.add_argument("--config", type=Path, required=True)
    for name in ('hook', 'management-hook'):
        hook = sub.add_parser(name)
        hook.add_argument("action", choices=("auth", "cleanup"))
        hook.add_argument("--config", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.operation in {'issue', 'issue-management'}:
            issuer = issue_management_certificate if args.operation == 'issue-management' else issue_certificate
            result = issuer(args.config)
        else:
            run_hook = management_certificate_hook if args.operation == 'management-hook' else certificate_hook
            result = {"status": run_hook(args.config, args.action)}
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception:
        print("certificate operation failed; preserve private state and reconcile", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
