#!/usr/bin/env python3
"""Write and verify the fixed Trivy policy used by image release scans."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import sys
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import NamedTuple


class TrivyException(NamedTuple):
    """One reviewed, temporary vulnerability-policy exception."""

    vulnerability_id: str
    purls: tuple[str, ...]
    expires_at: date
    statement: str


TRIVY_SCANNER_NAME = "Trivy"
TRIVY_VERSION = "v0.74.0"
_PERL_PURLS = (
    "pkg:deb/debian/libperl5.36",
    "pkg:deb/debian/perl",
    "pkg:deb/debian/perl-base",
    "pkg:deb/debian/perl-modules-5.36",
)
_PERL_EXCEPTION_STATEMENT = (
    "Reviewed 2026-09-14: no fixed Bookworm Perl package; temporary risk acceptance for pinned PostgreSQL 17.4 and Python pipeline compatibility fixtures. Trixie runtimes must use fixed Perl."
)
_POSTGRES_EXCEPTION_STATEMENT = (
    "Reviewed 2026-09-14: no fixed Bookworm package; temporary risk acceptance for pinned PostgreSQL 17.4 and Python pipeline compatibility dependencies."
)
TRIVY_CONFIG_BYTES = (
    b"exit-code: 1\n"
    b"db:\n"
    b"  repository:\n"
    b"    - ghcr.io/aquasecurity/trivy-db:2\n"
    b"    - public.ecr.aws/aquasecurity/trivy-db:2\n"
    b"pkg:\n"
    b"  types:\n"
    b"    - os\n"
    b"    - library\n"
    b"scan:\n"
    b"  scanners:\n"
    b"    - vuln\n"
    b"severity:\n"
    b"  - CRITICAL\n"
    b"timeout: 20m0s\n"
    b"vulnerability:\n"
    b"  ignore-unfixed: false\n"
)
TRIVY_EXCEPTIONS = (
    TrivyException(
        "CVE-2023-45853",
        ("pkg:deb/debian/zlib1g",),
        date(2026, 10, 14),
        (
            "Reviewed 2026-09-14: Debian Bookworm does not build the affected MiniZip code in zlib1g; temporary exception for pinned PostgreSQL 17.4 and Python pipeline compatibility fixtures."
        ),
    ),
    TrivyException(
        "CVE-2025-7458",
        ("pkg:deb/debian/libsqlite3-0",),
        date(2026, 10, 14),
        _POSTGRES_EXCEPTION_STATEMENT,
    ),
    TrivyException(
        "CVE-2026-13221",
        _PERL_PURLS,
        date(2026, 10, 14),
        _PERL_EXCEPTION_STATEMENT,
    ),
    TrivyException(
        "CVE-2026-42496",
        _PERL_PURLS,
        date(2026, 10, 14),
        _PERL_EXCEPTION_STATEMENT,
    ),
    TrivyException(
        "CVE-2026-43185",
        ("pkg:deb/debian/linux-libc-dev",),
        date(2026, 10, 14),
        (
            "Reviewed 2026-09-14: stable Debian has no fixed source package; agent compiler needs linux-libc-dev headers, not the affected ksmbd kernel runtime. Temporary exception does not attest host kernel safety."
        ),
    ),
    TrivyException(
        "CVE-2026-6653",
        ("pkg:deb/debian/libxml2",),
        date(2026, 10, 14),
        _POSTGRES_EXCEPTION_STATEMENT,
    ),
    TrivyException(
        "CVE-2026-8376",
        _PERL_PURLS,
        date(2026, 10, 14),
        _PERL_EXCEPTION_STATEMENT,
    ),
)


def _render_ignore_bytes(exceptions: tuple[TrivyException, ...]) -> bytes:
    rendered_lines = ["vulnerabilities:"]
    for exception in exceptions:
        rendered_lines.extend(
            (
                f"  - id: {exception.vulnerability_id}",
                "    purls:",
                *(f'      - "{purl}"' for purl in exception.purls),
                f"    expired_at: {exception.expires_at.isoformat()}",
                f"    statement: {json.dumps(exception.statement, ensure_ascii=True)}",
            )
        )
    return ("\n".join(rendered_lines) + "\n").encode("ascii")


TRIVY_IGNORE_BYTES = _render_ignore_bytes(TRIVY_EXCEPTIONS)
TRIVY_NO_EXCEPTIONS_BYTES = b"vulnerabilities: []\n"
TRIVY_CONFIG_SHA256 = "62844e2d381e0fa0f02bd63bcf43f8da3173e8fa8e585eb2bc25cef928575a36"
TRIVY_IGNORE_SHA256 = "2fe828560a3df13427bff6634f08509ec6be55ca75de9fb7e3fd095805a28c12"


class TrivyPolicyError(RuntimeError):
    """The controlled release scan policy could not be materialized exactly."""


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _verify_constant_hashes() -> None:
    if _sha256(TRIVY_CONFIG_BYTES) != TRIVY_CONFIG_SHA256:
        raise TrivyPolicyError("controlled Trivy configuration constant is invalid")
    if _sha256(TRIVY_IGNORE_BYTES) != TRIVY_IGNORE_SHA256:
        raise TrivyPolicyError("controlled Trivy ignore constant is invalid")


def _verify_exception_policy(today: date) -> None:
    declared_ids = tuple(exception.vulnerability_id for exception in TRIVY_EXCEPTIONS)
    if (
        declared_ids != tuple(sorted(set(declared_ids)))
        or _render_ignore_bytes(TRIVY_EXCEPTIONS) != TRIVY_IGNORE_BYTES
    ):
        raise TrivyPolicyError("controlled Trivy exceptions are inconsistent")
    for exception in TRIVY_EXCEPTIONS:
        if re.fullmatch(r"CVE-[0-9]{4}-[0-9]{4,}", exception.vulnerability_id) is None:
            raise TrivyPolicyError("controlled Trivy exception identifier is invalid")
        if (
            not exception.purls
            or exception.purls != tuple(sorted(set(exception.purls)))
            or any(
                re.fullmatch(r"pkg:deb/debian/[a-z0-9][a-z0-9.+-]*", purl) is None
                for purl in exception.purls
            )
        ):
            raise TrivyPolicyError("controlled Trivy exception packages are invalid")
        if not exception.statement or "\n" in exception.statement:
            raise TrivyPolicyError("controlled Trivy exception statement is invalid")
        if today >= exception.expires_at:
            raise TrivyPolicyError(
                f"controlled Trivy exception expired: {exception.vulnerability_id}"
            )


def _write_exact_regular_file(path: Path, payload: bytes, expected_sha256: str) -> None:
    if not path.is_absolute():
        raise TrivyPolicyError("controlled Trivy policy paths must be absolute")
    parent = path.parent
    parent_mode = parent.lstat().st_mode
    if not stat.S_ISDIR(parent_mode) or parent.is_symlink():
        raise TrivyPolicyError("controlled Trivy policy parent is invalid")

    temporary_path: Path | None = None
    try:
        descriptor, raw_temporary_path = tempfile.mkstemp(
            prefix=f".{path.name}.",
            dir=parent,
        )
        temporary_path = Path(raw_temporary_path)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        temporary_path.chmod(0o644)
        os.replace(temporary_path, path)
        temporary_path = None
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)

    metadata = path.lstat()
    observed = path.read_bytes()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or path.is_symlink()
        or observed != payload
        or _sha256(observed) != expected_sha256
    ):
        raise TrivyPolicyError("controlled Trivy policy verification failed")


def write_release_policy(
    config_file: Path,
    ignore_file: Path,
    *,
    today: date | None = None,
    use_exceptions: bool = True,
) -> None:
    """Atomically write and verify the reviewed config and temporary exceptions."""

    _verify_constant_hashes()
    if use_exceptions:
        _verify_exception_policy(today or datetime.now(UTC).date())
    if config_file == ignore_file:
        raise TrivyPolicyError("controlled Trivy policy paths must be distinct")
    if ignore_file.suffix not in {".yaml", ".yml"}:
        raise TrivyPolicyError("controlled Trivy ignore path must select YAML parsing")
    _write_exact_regular_file(config_file, TRIVY_CONFIG_BYTES, TRIVY_CONFIG_SHA256)
    ignore_bytes = TRIVY_IGNORE_BYTES if use_exceptions else TRIVY_NO_EXCEPTIONS_BYTES
    _write_exact_regular_file(ignore_file, ignore_bytes, _sha256(ignore_bytes))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config-file", type=Path, required=True)
    parser.add_argument("--ignore-file", type=Path, required=True)
    parser.add_argument(
        "--no-exceptions", action="store_true",
        help="Use an empty ignore file instead of the reviewed legacy exceptions.",
    )
    arguments = parser.parse_args()
    try:
        write_release_policy(
            arguments.config_file, arguments.ignore_file,
            use_exceptions=not arguments.no_exceptions,
        )
    except (OSError, TrivyPolicyError):
        sys.stderr.write("error: controlled Trivy policy generation failed\n")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()


__all__ = [
    "TRIVY_CONFIG_BYTES",
    "TRIVY_CONFIG_SHA256",
    "TRIVY_EXCEPTIONS",
    "TRIVY_IGNORE_BYTES",
    "TRIVY_IGNORE_SHA256",
    "TRIVY_SCANNER_NAME",
    "TRIVY_VERSION",
    "TrivyException",
    "TrivyPolicyError",
    "write_release_policy",
]
