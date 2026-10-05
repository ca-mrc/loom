from __future__ import annotations

import argparse
import json
import os
import subprocess
from dataclasses import asdict
from pathlib import Path
from urllib.parse import urljoin

import pytest
from scripts.ops.frontend_route_smoke import (
    HttpResponse,
    RouteCheck,
    _parse_route,
    _to_http_response,
    check_route,
    extract_asset_urls,
    validate_canonical_redirect,
    validate_config_document,
    validate_executable_shell,
)

ROUTE_ARGS = [
    "--route",
    "staging=https://yylx.world/dev=https://yylx.world/dev/api",
]


def _run_runtime_config(
    tmp_path: Path,
    *,
    environment: str,
    route_path: str,
    rehearsal_id: str = "",
    build_revision: str | None = None,
    source_ref: str | None = None,
    build_time_path: Path | None = None,
) -> tuple[dict[str, object], str]:
    config_path = tmp_path / "loom-frontend-config.json"
    template_path = tmp_path / "index.html.template"
    index_path = tmp_path / "index.html"
    template_path.write_text(
        '<link rel="stylesheet" href="./assets/index.css">'
        '<script type="module" src="./assets/index.js"></script>',
        encoding="utf-8",
    )
    env = {
        **os.environ,
        "LOOM_FRONTEND_CONFIG_PATH": str(config_path),
        "LOOM_FRONTEND_INDEX_TEMPLATE_PATH": str(template_path),
        "LOOM_FRONTEND_INDEX_PATH": str(index_path),
        "LOOM_FRONTEND_ENVIRONMENT": environment,
        "LOOM_FRONTEND_ENVIRONMENT_LABEL": environment.title(),
        "LOOM_FRONTEND_ROUTE_PATH": route_path,
        "LOOM_FRONTEND_API_BASE": route_path,
        "LOOM_FRONTEND_PUBLIC_ORIGIN": "https://yylx.world",
        "LOOM_FRONTEND_REHEARSAL_ID": rehearsal_id,
        # #2009: point the build-time file at a test path so the script
        # never needs /etc write access; a missing path is the normal
        # local/dev shape and must not fail the run (asserted below).
        "LOOM_FRONTEND_BUILD_TIME_PATH": str(
            build_time_path or (tmp_path / "missing-build-time"),
        ),
    }
    if build_revision is not None:
        env["LOOM_FRONTEND_BUILD_REVISION"] = build_revision
    if source_ref is not None:
        env["LOOM_FRONTEND_SOURCE_REF"] = source_ref
    subprocess.run(
        ["sh", "deploy/web-runtime-config.sh"],
        check=True,
        env=env,
        text=True,
        capture_output=True,
    )
    return json.loads(config_path.read_text(encoding="utf-8")), index_path.read_text(
        encoding="utf-8",
    )


def test_runtime_config_reports_build_metadata_for_focus_staleness_check(
    tmp_path: Path,
) -> None:
    """#2009: this is the "currently served build" signal an open tab
    polls on focus — independent of whatever build its own JS bundle
    already loaded."""
    build_time_path = tmp_path / "build-time"
    build_time_path.write_text("2026-09-20T08:00:00Z\n", encoding="utf-8")
    config, _html = _run_runtime_config(
        tmp_path,
        environment="development",
        route_path="/dev",
        build_revision="a" * 40,
        source_ref="refs/heads/dev",
        build_time_path=build_time_path,
    )

    assert config["buildRevision"] == "a" * 40
    assert config["sourceRef"] == "refs/heads/dev"
    assert config["buildTime"] == "2026-09-20T08:00:00Z"


def test_runtime_config_reports_honest_unknown_build_metadata_when_absent(
    tmp_path: Path,
) -> None:
    """A local/dev image built without --build-arg must not fail the
    entrypoint or fabricate a build identity."""
    config, _html = _run_runtime_config(
        tmp_path,
        environment="development",
        route_path="/dev",
    )

    assert config["buildRevision"] == "unknown"
    assert config["sourceRef"] == "unknown"
    assert config["buildTime"] == ""


def test_runtime_config_preserves_personal_source_without_commit_attribution(tmp_path, monkeypatch):
    monkeypatch.setenv("LOOM_FRONTEND_BUILD_KIND", "personal")
    monkeypatch.setenv("LOOM_FRONTEND_SOURCE_DIGEST", "sha256:" + "b" * 64)
    monkeypatch.setenv("LOOM_FRONTEND_SOURCE_BASE_COMMIT", "a" * 40)
    config, _ = _run_runtime_config(tmp_path, environment="development", route_path="/dev", build_revision="a" * 40)
    assert config["buildKind"] == "personal"
    assert config["sourceDigest"] == "sha256:" + "b" * 64
    assert config["sourceBaseCommit"] == "a" * 40
    assert config["buildRevision"] == "unknown"


def test_runtime_config_accepts_only_exact_staging_rehearsal_route(tmp_path: Path) -> None:
    isolation = "a" * 24
    route = f"/staging/rehearsal/{isolation}"
    config, html = _run_runtime_config(
        tmp_path,
        environment="staging",
        route_path=route,
        rehearsal_id=isolation,
    )

    assert config["routePath"] == route
    assert config["apiRouteBase"] == f"https://yylx.world{route}/api"
    assert f'href="{route}/assets/' in html
    assert f'src="{route}/assets/' in html


def test_runtime_config_rejects_stale_dev_staging_rehearsal_route(tmp_path: Path) -> None:
    # Staging serves /staging; the pre-migration /dev rehearsal route is rejected.
    isolation = "a" * 24
    with pytest.raises(subprocess.CalledProcessError):
        _run_runtime_config(
            tmp_path,
            environment="staging",
            route_path=f"/dev/rehearsal/{isolation}",
            rehearsal_id=isolation,
        )


def test_runtime_config_accepts_staging_deploy_route(tmp_path: Path) -> None:
    config, _html = _run_runtime_config(
        tmp_path,
        environment="staging",
        route_path="/staging",
    )
    assert config["routePath"] == "/staging"


def test_runtime_config_rejects_dev_route_for_staging_deploy(tmp_path: Path) -> None:
    with pytest.raises(subprocess.CalledProcessError):
        _run_runtime_config(
            tmp_path,
            environment="staging",
            route_path="/dev",
        )


def test_runtime_config_rejects_unbound_rehearsal_route(tmp_path: Path) -> None:
    with pytest.raises(subprocess.CalledProcessError):
        _run_runtime_config(
            tmp_path,
            environment="staging",
            route_path="/staging/rehearsal/" + "a" * 24,
        )


def test_validate_config_document_accepts_expected_prod_metadata() -> None:
    errors = validate_config_document(
        route_url="https://yylx.world/prod",
        expected_environment="production",
        expected_api_base="https://yylx.world/prod/api",
        cache_control="no-store, must-revalidate",
        document={
            "environment": "production",
            "environmentLabel": "Production",
            "routePath": "/prod",
            "apiBase": "/prod",
            "apiRouteBase": "https://yylx.world/prod/api",
        },
    )

    assert errors == []


def test_validate_config_document_rejects_cross_environment_api_base() -> None:
    errors = validate_config_document(
        route_url="https://yylx.world/dev",
        expected_environment="development",
        expected_api_base="https://yylx.world/dev/api",
        cache_control="max-age=3600",
        document={
            "environment": "development",
            "environmentLabel": "Development / staging",
            "routePath": "/dev",
            "apiBase": "/prod",
            "apiRouteBase": "https://yylx.world/prod/api",
        },
    )

    assert "apiRouteBase must be https://yylx.world/dev/api" in errors
    assert "apiBase must match routePath /dev" in errors
    assert "runtime config response must be no-store" in errors


@pytest.mark.parametrize(
    "cache_control",
    [
        "public, no-store=0",
        "max-age=0, no-store = secret",
    ],
)
def test_validate_config_document_rejects_valued_no_store_directive(
    cache_control: str,
) -> None:
    errors = validate_config_document(
        route_url="https://yylx.world/dev",
        expected_environment="staging",
        expected_api_base="https://yylx.world/dev/api",
        cache_control=cache_control,
        document={
            "environment": "staging",
            "environmentLabel": "Development / staging",
            "routePath": "/dev",
            "apiBase": "/dev",
            "apiRouteBase": "https://yylx.world/dev/api",
        },
    )

    assert "runtime config response must be no-store" in errors


def test_validate_config_document_accepts_trimmed_case_insensitive_no_store() -> None:
    errors = validate_config_document(
        route_url="https://yylx.world/dev",
        expected_environment="staging",
        expected_api_base="https://yylx.world/dev/api",
        cache_control="public,   No-StOrE   ",
        document={
            "environment": "staging",
            "environmentLabel": "Development / staging",
            "routePath": "/dev",
            "apiBase": "/dev",
            "apiRouteBase": "https://yylx.world/dev/api",
        },
    )

    assert errors == []


def test_validate_executable_shell_accepts_prefixed_javascript_and_css() -> None:
    shell = HttpResponse(
        url="https://yylx.world/dev/",
        status=200,
        headers={"content-type": "text/html; charset=utf-8"},
        body=(
            b'<link rel="stylesheet" href="/dev/assets/index.css">'
            b'<script type="module" src="/dev/assets/index.js"></script>'
        ),
    )
    assets = [
        HttpResponse(
            url="https://yylx.world/dev/assets/index.js",
            status=200,
            headers={"content-type": "application/javascript"},
            body=b"export {};",
        ),
        HttpResponse(
            url="https://yylx.world/dev/assets/index.css",
            status=200,
            headers={"content-type": "text/css"},
            body=b"#root {}",
        ),
    ]

    assert (
        validate_executable_shell(
            route_url="https://yylx.world/dev",
            shell=shell,
            assets=assets,
        )
        == []
    )


def test_validate_executable_shell_accepts_staging_prefixed_assets() -> None:
    # /staging is a first-class route surface (#857/#873), not just /dev + /prod.
    shell = HttpResponse(
        url="https://yylx.world/staging/",
        status=200,
        headers={"content-type": "text/html; charset=utf-8"},
        body=(
            b'<link rel="stylesheet" href="/staging/assets/index.css">'
            b'<script type="module" src="/staging/assets/index.js"></script>'
        ),
    )
    assets = [
        HttpResponse(
            url="https://yylx.world/staging/assets/index.js",
            status=200,
            headers={"content-type": "application/javascript"},
            body=b"export {};",
        ),
        HttpResponse(
            url="https://yylx.world/staging/assets/index.css",
            status=200,
            headers={"content-type": "text/css"},
            body=b"#root {}",
        ),
    ]

    assert (
        validate_executable_shell(
            route_url="https://yylx.world/staging",
            shell=shell,
            assets=assets,
        )
        == []
    )


def test_validate_executable_shell_rejects_asset_html_fallback() -> None:
    shell = HttpResponse(
        url="https://yylx.world/dev/",
        status=200,
        headers={"content-type": "text/html"},
        body=b'<script type="module" src="/dev/assets/index.js"></script>',
    )
    html_asset = HttpResponse(
        url="https://yylx.world/dev/assets/index.js",
        status=200,
        headers={"content-type": "text/html; charset=utf-8"},
        body=b'<div id="root"></div>',
    )

    errors = validate_executable_shell(
        route_url="https://yylx.world/dev",
        shell=shell,
        assets=[html_asset],
    )

    assert any("asset returned HTML fallback" in error for error in errors)


def test_validate_canonical_redirect_accepts_query_preserving_308() -> None:
    response = HttpResponse(
        url="https://yylx.world/dev?next=%2Fmonitor&x=1",
        status=308,
        headers={"Location": "/dev/?next=%2Fmonitor&x=1"},
        body=b"",
    )

    assert (
        validate_canonical_redirect(
            route_url="https://yylx.world/dev?next=%2Fmonitor&x=1",
            response=response,
        )
        == []
    )


def test_validate_canonical_redirect_rejects_non_308_and_dropped_query() -> None:
    response = HttpResponse(
        url="https://yylx.world/dev?next=%2Fmonitor&x=1",
        status=301,
        headers={"location": "/dev/"},
        body=b"",
    )

    errors = validate_canonical_redirect(
        route_url="https://yylx.world/dev?next=%2Fmonitor&x=1",
        response=response,
    )

    assert "exact route returned HTTP 301; expected 308" in errors
    assert "canonical redirect Location must be /dev/?next=%2Fmonitor&x=1" in errors


@pytest.mark.parametrize(
    "locations",
    [
        ("https://evil.example/location-secret", "/dev/?next=%2Fmonitor&x=1"),
        ("/dev/?next=%2Fmonitor&x=1", "https://evil.example/location-secret"),
    ],
)
def test_validate_canonical_redirect_rejects_duplicate_location_without_leak(
    locations: tuple[str, str],
) -> None:
    response = HttpResponse(
        url="https://yylx.world/dev?next=%2Fmonitor&x=1",
        status=308,
        headers={"location": locations},
        body=b"",
    )

    errors = validate_canonical_redirect(
        route_url="https://yylx.world/dev?next=%2Fmonitor&x=1",
        response=response,
    )

    assert errors == ["exact route must return exactly one Location header"]
    assert "location-secret" not in json.dumps(errors)


def test_to_http_response_preserves_all_values_from_http_message() -> None:
    class FakeHeaders:
        def __init__(self) -> None:
            self.values = {
                "location": ["https://evil.example/first", "/dev/"],
                "content-type": ["text/html", "application/javascript"],
                "cache-control": ["max-age=0", "no-store"],
            }

        def keys(self) -> list[str]:
            return [
                "Location",
                "Location",
                "Content-Type",
                "Content-Type",
                "Cache-Control",
                "Cache-Control",
            ]

        def get_all(self, name: str) -> list[str] | None:
            return self.values.get(name.lower())

    class FakeResponse:
        def __init__(self) -> None:
            self.headers = FakeHeaders()

        def geturl(self) -> str:
            return "https://yylx.world/dev"

        def getcode(self) -> int:
            return 308

        def read(self) -> bytes:
            return b""

    converted = _to_http_response("https://yylx.world/dev", FakeResponse())

    assert converted.headers == {
        "location": ("https://evil.example/first", "/dev/"),
        "content-type": ("text/html", "application/javascript"),
        "cache-control": ("max-age=0", "no-store"),
    }


def test_extract_asset_urls_ignores_near_and_data_attributes() -> None:
    body = (
        b'<script data-src="/dev/assets/decoy.js"></script>'
        b'<script type="module" src = "/dev/assets/index.js"></script>'
        b'<link rel="stylesheet" href="/dev/assets/index.css">'
        b"<script src=/dev/assets/unquoted.js></script>"
        b'<script src="/dev/assets/index.js.map"></script>'
        b'<script src="/dev/assets/index.jsx"></script>'
    )

    assert extract_asset_urls("https://yylx.world/dev/", body) == [
        "https://yylx.world/dev/assets/index.css",
        "https://yylx.world/dev/assets/index.js",
        "https://yylx.world/dev/assets/unquoted.js",
    ]


def test_extract_asset_urls_ignores_wrong_html_elements_and_link_rel() -> None:
    body = (
        b'<img src="/dev/assets/image.js">'
        b'<a href="/dev/assets/download.css">download</a>'
        b'<link rel="preload" href="/dev/assets/preload.css">'
        b'<script src="/dev/assets/index.js"></script>'
        b'<link rel="preload stylesheet" href="/dev/assets/index.css">'
    )

    assert extract_asset_urls("https://yylx.world/dev/", body) == [
        "https://yylx.world/dev/assets/index.css",
        "https://yylx.world/dev/assets/index.js",
    ]


def test_extract_asset_urls_rejects_duplicate_src_and_href() -> None:
    body = (
        b'<script src="/dev/assets/first.js" '
        b'src="/dev/assets/second.js"></script>'
        b'<link rel="stylesheet" href="/dev/assets/first.css" '
        b'href="/dev/assets/second.css">'
    )

    assert extract_asset_urls("https://yylx.world/dev/", body) == []


@pytest.mark.parametrize(
    "duplicate_element",
    [
        '<script src src="/dev/assets/second.js"></script>',
        '<link rel="stylesheet" href href="/dev/assets/second.css">',
    ],
)
def test_validate_executable_shell_rejects_empty_first_duplicate_asset_value(
    duplicate_element: str,
) -> None:
    shell = HttpResponse(
        url="https://yylx.world/dev/",
        status=200,
        headers={"content-type": "text/html"},
        body=(f'<link rel="stylesheet" href="/dev/assets/index.css">{duplicate_element}').encode(),
    )
    stylesheet = HttpResponse(
        url="https://yylx.world/dev/assets/index.css",
        status=200,
        headers={"content-type": "text/css"},
        body=b"#root {}",
    )

    errors = validate_executable_shell(
        route_url="https://yylx.world/dev",
        shell=shell,
        assets=[stylesheet],
    )

    assert "shell contains unsafe asset reference" in errors


def test_extract_asset_urls_rejects_duplicate_rel_when_any_value_is_stylesheet() -> None:
    body = (
        b'<link rel="preload" rel="stylesheet" '
        b'href="/dev/assets/ignored.css">'
        b'<link rel="stylesheet" rel="preload" '
        b'href="/dev/assets/included.css">'
    )

    assert extract_asset_urls("https://yylx.world/dev/", body) == []


def test_extract_asset_urls_excludes_query_and_fragment_candidates() -> None:
    body = (
        b'<script src="/dev/assets/index.js?v=secret"></script>'
        b'<link rel="stylesheet" href="/dev/assets/index.css#private">'
    )

    assert extract_asset_urls("https://yylx.world/dev/", body) == []


def test_validate_executable_shell_ignores_cross_origin_asset() -> None:
    external_asset_url = "https://fonts.example/font.css?family=Loom"
    same_origin_asset_url = "https://yylx.world/dev/assets/index.js"
    shell = HttpResponse(
        url="https://yylx.world/dev/",
        status=200,
        headers={"content-type": "text/html"},
        body=(
            f'<link rel="stylesheet" href="{external_asset_url}">'
            '<script type="module" src="/dev/assets/index.js"></script>'
        ).encode(),
    )
    same_origin_asset = HttpResponse(
        url=same_origin_asset_url,
        status=200,
        headers={"content-type": "application/javascript"},
        body=b"export {};",
    )

    errors = validate_executable_shell(
        route_url="https://yylx.world/dev",
        shell=shell,
        assets=[same_origin_asset],
    )

    assert errors == []


@pytest.mark.parametrize(
    ("element", "asset_path"),
    [
        ("script", "/dev/assets/index.css"),
        ("stylesheet", "/dev/assets/index.js"),
    ],
)
def test_validate_executable_shell_rejects_element_extension_mismatch(
    element: str,
    asset_path: str,
) -> None:
    if element == "script":
        reference = f'<script src="{asset_path}"></script>'
    else:
        reference = f'<link rel="stylesheet" href="{asset_path}">'
    shell = HttpResponse(
        url="https://yylx.world/dev/",
        status=200,
        headers={"content-type": "text/html"},
        body=reference.encode(),
    )

    errors = validate_executable_shell(
        route_url="https://yylx.world/dev",
        shell=shell,
        assets=[],
    )

    assert "shell contains unsafe asset reference" in errors


@pytest.mark.parametrize(
    ("element", "asset_url", "content_type"),
    [
        (
            '<script src="/dev/assets/index.js"></script>',
            "https://yylx.world/dev/assets/index.js",
            "text/notjavascript",
        ),
        (
            '<link rel="stylesheet" href="/dev/assets/index.css">',
            "https://yylx.world/dev/assets/index.css",
            "text/css-not-valid",
        ),
        (
            '<link rel="stylesheet" href="/dev/assets/index.css">',
            "https://yylx.world/dev/assets/index.css",
            "text/css, text/plain",
        ),
    ],
)
def test_validate_executable_shell_requires_exact_mime_essence(
    element: str,
    asset_url: str,
    content_type: str,
) -> None:
    shell = HttpResponse(
        url="https://yylx.world/dev/",
        status=200,
        headers={"content-type": "text/html"},
        body=element.encode(),
    )
    asset = HttpResponse(
        url=asset_url,
        status=200,
        headers={"content-type": content_type},
        body=b"safe non-HTML asset body",
    )

    errors = validate_executable_shell(
        route_url="https://yylx.world/dev",
        shell=shell,
        assets=[asset],
    )

    assert f"asset has unexpected MIME: {asset_url}" in errors
    assert content_type not in json.dumps(errors)


@pytest.mark.parametrize(
    "content_types",
    [
        ("text/html; note=header-secret", "application/javascript"),
        ("application/javascript", "text/html; note=header-secret"),
    ],
)
def test_validate_executable_shell_rejects_duplicate_asset_content_type(
    content_types: tuple[str, str],
) -> None:
    asset_url = "https://yylx.world/dev/assets/index.js"
    shell = HttpResponse(
        url="https://yylx.world/dev/",
        status=200,
        headers={"content-type": "text/html"},
        body=b'<script src="/dev/assets/index.js"></script>',
    )
    asset = HttpResponse(
        url=asset_url,
        status=200,
        headers={"content-type": content_types},
        body=b"export {};",
    )

    errors = validate_executable_shell(
        route_url="https://yylx.world/dev",
        shell=shell,
        assets=[asset],
    )

    assert errors == [f"asset must return exactly one Content-Type header: {asset_url}"]
    assert "header-secret" not in json.dumps(errors)


@pytest.mark.parametrize(
    "content_types",
    [
        ("text/html", "application/javascript; note=shell-header-secret"),
        ("application/javascript; note=shell-header-secret", "text/html"),
    ],
)
def test_validate_executable_shell_rejects_duplicate_shell_content_type(
    content_types: tuple[str, str],
) -> None:
    asset_url = "https://yylx.world/dev/assets/index.js"
    shell = HttpResponse(
        url="https://yylx.world/dev/",
        status=200,
        headers={"content-type": content_types},
        body=b'<script src="/dev/assets/index.js"></script>',
    )
    asset = HttpResponse(
        url=asset_url,
        status=200,
        headers={"content-type": "application/javascript"},
        body=b"export {};",
    )

    errors = validate_executable_shell(
        route_url="https://yylx.world/dev",
        shell=shell,
        assets=[asset],
    )

    assert errors == ["canonical shell must return exactly one Content-Type header"]
    assert "shell-header-secret" not in json.dumps(errors)


def test_validate_executable_shell_requires_shell_and_asset_content_type() -> None:
    asset_url = "https://yylx.world/dev/assets/index.js"
    shell = HttpResponse(
        url="https://yylx.world/dev/",
        status=200,
        headers={},
        body=b'<script src="/dev/assets/index.js"></script>',
    )
    asset = HttpResponse(
        url=asset_url,
        status=200,
        headers={},
        body=b"export {};",
    )

    errors = validate_executable_shell(
        route_url="https://yylx.world/dev",
        shell=shell,
        assets=[asset],
    )

    assert "canonical shell must return exactly one Content-Type header" in errors
    assert f"asset must return exactly one Content-Type header: {asset_url}" in errors


def test_validate_executable_shell_requires_every_referenced_asset_response() -> None:
    asset_url = "https://yylx.world/dev/assets/index.js"
    shell = HttpResponse(
        url="https://yylx.world/dev/",
        status=200,
        headers={"content-type": "text/html"},
        body=b'<script type="module" src="/dev/assets/index.js"></script>',
    )

    errors = validate_executable_shell(
        route_url="https://yylx.world/dev",
        shell=shell,
        assets=[],
    )

    assert f"asset response missing: {asset_url}" in errors


def test_validate_executable_shell_rejects_nested_pseudo_asset() -> None:
    asset_url = "https://yylx.world/dev/cache/dev/assets/index.js"
    shell = HttpResponse(
        url="https://yylx.world/dev/library/batches/example-id",
        status=200,
        headers={"content-type": "text/html"},
        body=f'<script type="module" src="{asset_url}"></script>'.encode(),
    )
    asset = HttpResponse(
        url=asset_url,
        status=200,
        headers={"content-type": "application/javascript"},
        body=b"export {};",
    )

    errors = validate_executable_shell(
        route_url="https://yylx.world/dev",
        shell=shell,
        assets=[asset],
    )

    assert "shell contains unsafe asset reference" in errors
    assert all(asset_url not in error for error in errors)


def test_validate_executable_shell_rejects_html_body_with_javascript_mime() -> None:
    asset_url = "https://yylx.world/dev/assets/index.js"
    shell = HttpResponse(
        url="https://yylx.world/dev/monitor",
        status=200,
        headers={"content-type": "text/html"},
        body=b'<script type="module" src="/dev/assets/index.js"></script>',
    )
    disguised_html = HttpResponse(
        url=asset_url,
        status=200,
        headers={"content-type": "application/javascript"},
        body=b'<!doctype html><div id="root"></div>',
    )

    errors = validate_executable_shell(
        route_url="https://yylx.world/dev",
        shell=shell,
        assets=[disguised_html],
    )

    assert f"asset returned HTML fallback: {asset_url}" in errors


@pytest.mark.parametrize(
    "html_body",
    [
        b"\xef\xbb\xbf<!doctype html><html></html>",
        b"<!-- ingress fallback -->\n<html></html>",
        b"<head><title>Loom</title></head>",
        b"<body><div>Loading</div></body>",
        b'<div class="app" id="root"></div>',
    ],
)
def test_validate_executable_shell_rejects_bounded_html_document_shapes(
    html_body: bytes,
) -> None:
    asset_url = "https://yylx.world/dev/assets/index.js"
    shell = HttpResponse(
        url="https://yylx.world/dev/monitor",
        status=200,
        headers={"content-type": "text/html"},
        body=b'<script type="module" src="/dev/assets/index.js"></script>',
    )
    disguised_html = HttpResponse(
        url=asset_url,
        status=200,
        headers={"content-type": "application/javascript"},
        body=html_body,
    )

    errors = validate_executable_shell(
        route_url="https://yylx.world/dev",
        shell=shell,
        assets=[disguised_html],
    )

    assert f"asset returned HTML fallback: {asset_url}" in errors


def test_validate_executable_shell_rejects_html_beyond_parser_limit() -> None:
    asset_url = "https://yylx.world/dev/assets/index.js"
    shell = HttpResponse(
        url="https://yylx.world/dev/",
        status=200,
        headers={"content-type": "text/html"},
        body=(b'<script type="module" src="/dev/assets/index.js"></script>' + b" " * (1024 * 1024)),
    )
    asset = HttpResponse(
        url=asset_url,
        status=200,
        headers={"content-type": "application/javascript"},
        body=b"export {};",
    )

    errors = validate_executable_shell(
        route_url="https://yylx.world/dev",
        shell=shell,
        assets=[asset],
    )

    assert "shell exceeds HTML parsing limit" in errors


def _mock_dev_route_responses(
    *,
    route_url: str = "https://yylx.world/dev",
    javascript_response_url: str = "https://yylx.world/dev/assets/index.js",
    javascript_content_type: str = "application/javascript",
    javascript_body: bytes = b"export {};",
    shell_body: bytes | None = None,
) -> dict[str, HttpResponse]:
    redirect_url = f"{route_url}?next=%2Fmonitor&x=1"
    config_url = f"{route_url}/loom-frontend-config.json"
    shell_urls = [
        f"{route_url}/",
        f"{route_url}/monitor",
        f"{route_url}/batches/example-id",
        f"{route_url}/providers/example-id",
        f"{route_url}/library/batches/example-id",
    ]
    javascript_url = f"{route_url}/assets/index.js"
    stylesheet_url = f"{route_url}/assets/index.css"
    nested_pseudo_asset_url = (
        f"{route_url}/batches/assets/loom-route-smoke-nonexistent.js"
    )
    if shell_body is None:
        shell_body = (
            b'<link rel="stylesheet" href="/dev/assets/index.css">'
            b'<script type="module" src="/dev/assets/index.js"></script>'
        )
    return {
        redirect_url: HttpResponse(
            url=redirect_url,
            status=308,
            headers={"location": "/dev/?next=%2Fmonitor&x=1"},
            body=b"",
        ),
        config_url: HttpResponse(
            url=config_url,
            status=200,
            headers={
                "content-type": "application/json",
                "cache-control": "no-store",
            },
            body=json.dumps(
                {
                    "environment": "staging",
                    "environmentLabel": "Development / staging",
                    "routePath": "/dev",
                    "apiBase": "/dev",
                    "apiRouteBase": f"{route_url}/api",
                },
            ).encode(),
        ),
        **{
            url: HttpResponse(
                url=url,
                status=200,
                headers={"content-type": "text/html; charset=utf-8"},
                body=shell_body,
            )
            for url in shell_urls
        },
        javascript_url: HttpResponse(
            url=javascript_response_url,
            status=200,
            headers={"content-type": javascript_content_type},
            body=javascript_body,
        ),
        stylesheet_url: HttpResponse(
            url=stylesheet_url,
            status=200,
            headers={"content-type": "text/css"},
            body=b"#root {}",
        ),
        nested_pseudo_asset_url: HttpResponse(
            url=nested_pseudo_asset_url,
            status=404,
            headers={"content-type": "text/html"},
            body=b"",
        ),
    }


def _check_mock_dev_route(responses: dict[str, HttpResponse]) -> RouteCheck:
    def fetcher(
        url: str,
        *,
        timeout: float,
        method: str,
        follow_redirects: bool,
    ) -> HttpResponse:
        assert timeout == 3.0
        assert follow_redirects is False
        return responses[url]

    return check_route(
        route_url="https://yylx.world/dev",
        expected_environment="staging",
        expected_api_base="https://yylx.world/dev/api",
        timeout=3.0,
        fetcher=fetcher,
    )


def test_check_route_fetches_redirect_deep_shells_and_unique_assets() -> None:
    route_url = "https://yylx.world/dev"
    redirect_url = f"{route_url}?next=%2Fmonitor&x=1"
    config_url = f"{route_url}/loom-frontend-config.json"
    shell_urls = [
        f"{route_url}/",
        f"{route_url}/monitor",
        f"{route_url}/batches/example-id",
        f"{route_url}/providers/example-id",
        f"{route_url}/library/batches/example-id",
    ]
    javascript_url = f"{route_url}/assets/index.js"
    stylesheet_url = f"{route_url}/assets/index.css"
    nested_pseudo_asset_url = (
        f"{route_url}/batches/assets/loom-route-smoke-nonexistent.js"
    )
    responses = _mock_dev_route_responses()
    calls: list[tuple[str, str, bool]] = []

    def fetcher(
        url: str,
        *,
        timeout: float,
        method: str,
        follow_redirects: bool,
    ) -> HttpResponse:
        assert timeout == 3.0
        calls.append((method, url, follow_redirects))
        return responses[url]

    check = check_route(
        route_url=route_url,
        expected_environment="staging",
        expected_api_base="https://yylx.world/dev/api",
        timeout=3.0,
        fetcher=fetcher,
    )

    assert check.status == "pass"
    assert check.errors == []
    assert ("HEAD", redirect_url, False) in calls
    assert ("GET", redirect_url, False) in calls
    assert all(("GET", url, False) in calls for url in [config_url, *shell_urls])
    assert calls.count(("GET", javascript_url, False)) == 1
    assert calls.count(("GET", stylesheet_url, False)) == 1
    assert calls.count(("GET", nested_pseudo_asset_url, False)) == 1
    assert all(follow_redirects is False for _, _, follow_redirects in calls)
    evidence = asdict(check)
    assert {item["url"] for item in evidence["responses"]} == set(responses)
    redirect_evidence = [item for item in evidence["responses"] if item["url"] == redirect_url]
    assert {(item["method"], item["status"]) for item in redirect_evidence} == {
        ("HEAD", 308),
        ("GET", 308),
    }
    assert all(
        set(item) == {"url", "method", "status", "content_type"} for item in evidence["responses"]
    )


def test_check_route_rejects_nested_pseudo_asset_spa_fallback() -> None:
    pseudo_asset_url = (
        "https://yylx.world/dev/batches/assets/"
        "loom-route-smoke-nonexistent.js"
    )
    responses = _mock_dev_route_responses()
    responses[pseudo_asset_url] = HttpResponse(
        url=pseudo_asset_url,
        status=200,
        headers={"content-type": "text/html"},
        body=b'<!doctype html><div id="root"></div>',
    )

    check = _check_mock_dev_route(responses)

    assert check.status == "fail"
    assert (
        "nested pseudo-asset path returned HTTP 200; expected 404"
        in check.errors
    )


@pytest.mark.parametrize(
    "content_types",
    [
        ("text/html; note=config-header-secret", "application/json"),
        ("application/json", "text/html; note=config-header-secret"),
    ],
)
def test_check_route_rejects_duplicate_config_content_type_without_evidence_leak(
    content_types: tuple[str, str],
) -> None:
    config_url = "https://yylx.world/dev/loom-frontend-config.json"
    responses = _mock_dev_route_responses()
    original = responses[config_url]
    responses[config_url] = HttpResponse(
        url=original.url,
        status=original.status,
        headers={
            "content-type": content_types,
            "cache-control": "no-store",
        },
        body=original.body,
    )

    check = _check_mock_dev_route(responses)

    serialized = json.dumps(asdict(check))
    assert check.status == "fail"
    assert "runtime config must return exactly one Content-Type header" in check.errors
    assert "config-header-secret" not in serialized
    config_evidence = next(response for response in check.responses if response.url == config_url)
    assert config_evidence.content_type == ""


def test_check_route_requires_config_content_type() -> None:
    config_url = "https://yylx.world/dev/loom-frontend-config.json"
    responses = _mock_dev_route_responses()
    original = responses[config_url]
    responses[config_url] = HttpResponse(
        url=original.url,
        status=original.status,
        headers={"cache-control": "no-store"},
        body=original.body,
    )

    check = _check_mock_dev_route(responses)

    assert check.status == "fail"
    assert "runtime config must return exactly one Content-Type header" in check.errors


def test_check_route_combines_cache_control_and_redacts_secret_content_type() -> None:
    config_url = "https://yylx.world/dev/loom-frontend-config.json"
    javascript_url = "https://yylx.world/dev/assets/index.js"
    header_secret = "sk-header-secret-value"
    responses = _mock_dev_route_responses()
    config = responses[config_url]
    responses[config_url] = HttpResponse(
        url=config.url,
        status=config.status,
        headers={
            "content-type": "application/json",
            "cache-control": ("public, max-age=0", "  No-StOrE  "),
        },
        body=config.body,
    )
    javascript = responses[javascript_url]
    responses[javascript_url] = HttpResponse(
        url=javascript.url,
        status=javascript.status,
        headers={
            "content-type": f"application/javascript; note={header_secret}",
        },
        body=javascript.body,
    )

    check = _check_mock_dev_route(responses)

    serialized = json.dumps(asdict(check))
    assert check.status == "pass"
    assert header_secret not in serialized
    javascript_evidence = next(
        response for response in check.responses if response.url == javascript_url
    )
    assert javascript_evidence.content_type == ""


def test_check_route_redacts_transport_url_and_html_fallback_body() -> None:
    signed_final_url = "https://cdn.example/index.js?X-Amz-Signature=do-not-serialize"
    responses = _mock_dev_route_responses(
        javascript_response_url=signed_final_url,
        javascript_content_type="text/html; charset=utf-8",
        javascript_body=b'<div id="root">do-not-serialize-body</div>',
    )

    def fetcher(
        url: str,
        *,
        timeout: float,
        method: str,
        follow_redirects: bool,
    ) -> HttpResponse:
        assert timeout == 3.0
        return responses[url]

    check = check_route(
        route_url="https://yylx.world/dev",
        expected_environment="staging",
        expected_api_base="https://yylx.world/dev/api",
        timeout=3.0,
        fetcher=fetcher,
    )

    serialized = json.dumps(asdict(check))
    assert check.status == "fail"
    assert any(
        "asset returned HTML fallback: https://yylx.world/dev/assets/index.js" in error
        for error in check.errors
    )
    assert "X-Amz-Signature" not in serialized
    assert "cdn.example" not in serialized
    assert "do-not-serialize-body" not in serialized


def test_check_route_does_not_follow_or_serialize_asset_redirect() -> None:
    asset_url = "https://yylx.world/dev/assets/index.js"
    redirect_target = "https://cdn.example/index.js?secret=do-not-serialize"
    responses = _mock_dev_route_responses()
    responses[asset_url] = HttpResponse(
        url=redirect_target,
        status=302,
        headers={
            "location": redirect_target,
            "content-type": "text/html; charset=utf-8",
        },
        body=b"redirect body must not be serialized",
    )
    calls: list[tuple[str, str, bool]] = []

    def fetcher(
        url: str,
        *,
        timeout: float,
        method: str,
        follow_redirects: bool,
    ) -> HttpResponse:
        assert timeout == 3.0
        calls.append((method, url, follow_redirects))
        return responses[url]

    check = check_route(
        route_url="https://yylx.world/dev",
        expected_environment="staging",
        expected_api_base="https://yylx.world/dev/api",
        timeout=3.0,
        fetcher=fetcher,
    )

    serialized = json.dumps(asdict(check))
    assert check.status == "fail"
    assert any(f"asset returned HTTP 302: {asset_url}" in error for error in check.errors)
    assert all(follow_redirects is False for _, _, follow_redirects in calls)
    assert not any(url == redirect_target for _, url, _ in calls)
    assert redirect_target not in serialized
    assert "redirect body must not be serialized" not in serialized


def test_check_route_rejects_userinfo_asset_refs_without_fetch_or_leak() -> None:
    same_origin_secret = "same-origin-secret"
    cross_origin_secret = "cross-origin-secret"
    shell_body = (
        b'<link rel="stylesheet" href="/dev/assets/index.css">'
        + (
            '<script type="module" '
            f'src="https://user:{same_origin_secret}@yylx.world/dev/assets/app.js">'
            "</script>"
            '<script type="module" '
            f'src="https://user:{cross_origin_secret}@cdn.example/dev/assets/app.js">'
            "</script>"
        ).encode()
    )
    responses = _mock_dev_route_responses(shell_body=shell_body)
    calls: list[str] = []

    def fetcher(
        url: str,
        *,
        timeout: float,
        method: str,
        follow_redirects: bool,
    ) -> HttpResponse:
        assert timeout == 3.0
        calls.append(url)
        return responses[url]

    check = check_route(
        route_url="https://yylx.world/dev",
        expected_environment="staging",
        expected_api_base="https://yylx.world/dev/api",
        timeout=3.0,
        fetcher=fetcher,
    )

    serialized = json.dumps(asdict(check))
    assert check.status == "fail"
    assert any("shell contains unsafe asset reference" in error for error in check.errors)
    assert all(same_origin_secret not in url for url in calls)
    assert all(cross_origin_secret not in url for url in calls)
    assert same_origin_secret not in serialized
    assert cross_origin_secret not in serialized


@pytest.mark.parametrize(
    ("route_url", "asset_url"),
    [
        (
            "https://yylx.world/dev",
            "https://yylx.world:0/styles/zero-port.css",
        ),
        (
            "https://yylx.world:0/dev",
            "https://yylx.world/styles/default-port.css",
        ),
    ],
)
def test_check_route_ignores_clean_cross_origin_stylesheet_with_explicit_port(
    route_url: str,
    asset_url: str,
) -> None:
    shell_body = (
        b'<link rel="stylesheet" href="/dev/assets/index.css">'
        + f'<link rel="stylesheet" href="{asset_url}">'.encode()
    )
    responses = _mock_dev_route_responses(
        route_url=route_url,
        javascript_response_url=f"{route_url}/assets/index.js",
        shell_body=shell_body,
    )
    calls: list[str] = []

    def fetcher(
        url: str,
        *,
        timeout: float,
        method: str,
        follow_redirects: bool,
    ) -> HttpResponse:
        assert timeout == 3.0
        calls.append(url)
        return responses[url]

    check = check_route(
        route_url=route_url,
        expected_environment="staging",
        expected_api_base=f"{route_url}/api",
        timeout=3.0,
        fetcher=fetcher,
    )

    serialized = json.dumps(asdict(check))
    assert check.status == "pass"
    assert asset_url not in calls
    assert not any(response.url == asset_url for response in check.responses)
    assert asset_url not in serialized


def test_check_route_rejects_query_and_fragment_asset_refs_without_leak() -> None:
    query_secret = "query-secret"
    fragment_secret = "fragment-secret"
    shell_body = (
        b'<link rel="stylesheet" href="/dev/assets/index.css">'
        + (
            f'<script src="/dev/assets/app.js?token={query_secret}"></script>'
            f'<link rel="stylesheet" '
            f'href="/dev/assets/theme.css#{fragment_secret}">'
        ).encode()
    )
    responses = _mock_dev_route_responses(shell_body=shell_body)
    calls: list[str] = []

    def fetcher(
        url: str,
        *,
        timeout: float,
        method: str,
        follow_redirects: bool,
    ) -> HttpResponse:
        assert timeout == 3.0
        calls.append(url)
        return responses[url]

    check = check_route(
        route_url="https://yylx.world/dev",
        expected_environment="staging",
        expected_api_base="https://yylx.world/dev/api",
        timeout=3.0,
        fetcher=fetcher,
    )

    serialized = json.dumps(asdict(check))
    assert check.status == "fail"
    assert any("shell contains unsafe asset reference" in error for error in check.errors)
    assert all(query_secret not in url for url in calls)
    assert all(fragment_secret not in url for url in calls)
    assert query_secret not in serialized
    assert fragment_secret not in serialized


@pytest.mark.parametrize(
    "unsafe_ref",
    [
        "https://yylx.world/dev/nested/assets/noncanonical.js",
        "https://yylx.world/dev/assets/not-javascript.jsx",
    ],
)
def test_check_route_rejects_noncanonical_asset_candidates_without_leak(
    unsafe_ref: str,
) -> None:
    stylesheet_url = "https://yylx.world/dev/assets/index.css"
    shell_body = (
        b'<link rel="stylesheet" href="/dev/assets/index.css">'
        + f'<script src="{unsafe_ref}"></script>'.encode()
    )
    responses = _mock_dev_route_responses(shell_body=shell_body)
    calls: list[str] = []

    def fetcher(
        url: str,
        *,
        timeout: float,
        method: str,
        follow_redirects: bool,
    ) -> HttpResponse:
        assert timeout == 3.0
        calls.append(url)
        return responses[url]

    check = check_route(
        route_url="https://yylx.world/dev",
        expected_environment="staging",
        expected_api_base="https://yylx.world/dev/api",
        timeout=3.0,
        fetcher=fetcher,
    )

    serialized = json.dumps(asdict(check))
    assert check.status == "fail"
    assert any("shell contains unsafe asset reference" in error for error in check.errors)
    assert stylesheet_url in calls
    assert unsafe_ref not in calls
    assert unsafe_ref not in serialized


@pytest.mark.parametrize(
    "external_ref",
    [
        "http://yylx.world/dev/assets/insecure.js",
        "https://cdn.example/dev/assets/cross-origin.js?variant=1",
    ],
)
def test_check_route_rejects_cross_origin_script_candidates_without_fetch(
    external_ref: str,
) -> None:
    stylesheet_url = "https://yylx.world/dev/assets/index.css"
    shell_body = (
        b'<link rel="stylesheet" href="/dev/assets/index.css">'
        + f'<script src="{external_ref}"></script>'.encode()
    )
    responses = _mock_dev_route_responses(shell_body=shell_body)
    calls: list[str] = []

    def fetcher(
        url: str,
        *,
        timeout: float,
        method: str,
        follow_redirects: bool,
    ) -> HttpResponse:
        assert timeout == 3.0
        calls.append(url)
        return responses[url]

    check = check_route(
        route_url="https://yylx.world/dev",
        expected_environment="staging",
        expected_api_base="https://yylx.world/dev/api",
        timeout=3.0,
        fetcher=fetcher,
    )

    serialized = json.dumps(asdict(check))
    assert check.status == "fail"
    assert any("shell contains unsafe asset reference" in error for error in check.errors)
    assert stylesheet_url in calls
    assert external_ref not in calls
    assert external_ref not in serialized


def test_check_route_ignores_clean_cross_origin_stylesheet_without_fetch() -> None:
    external_ref = "https://fonts.example/css2?family=Loom"
    stylesheet_url = "https://yylx.world/dev/assets/index.css"
    shell_body = (
        b'<link rel="stylesheet" href="/dev/assets/index.css">'
        + f'<link rel="stylesheet" href="{external_ref}">'.encode()
    )
    responses = _mock_dev_route_responses(shell_body=shell_body)
    calls: list[str] = []

    def fetcher(
        url: str,
        *,
        timeout: float,
        method: str,
        follow_redirects: bool,
    ) -> HttpResponse:
        assert timeout == 3.0
        calls.append(url)
        return responses[url]

    check = check_route(
        route_url="https://yylx.world/dev",
        expected_environment="staging",
        expected_api_base="https://yylx.world/dev/api",
        timeout=3.0,
        fetcher=fetcher,
    )

    serialized = json.dumps(asdict(check))
    assert check.status == "pass"
    assert stylesheet_url in calls
    assert external_ref not in calls
    assert external_ref not in serialized


def test_check_route_rejects_secret_looking_cross_origin_stylesheet_without_leak() -> None:
    external_ref = "https://fonts.example/css2?family=Loom&X-Amz-Signature=external-secret-value"
    shell_body = (
        b'<link rel="stylesheet" href="/dev/assets/index.css">'
        + f'<link rel="stylesheet" href="{external_ref}">'.encode()
    )
    responses = _mock_dev_route_responses(shell_body=shell_body)
    calls: list[str] = []

    def fetcher(
        url: str,
        *,
        timeout: float,
        method: str,
        follow_redirects: bool,
    ) -> HttpResponse:
        assert timeout == 3.0
        calls.append(url)
        return responses[url]

    check = check_route(
        route_url="https://yylx.world/dev",
        expected_environment="staging",
        expected_api_base="https://yylx.world/dev/api",
        timeout=3.0,
        fetcher=fetcher,
    )

    serialized = json.dumps(asdict(check))
    assert check.status == "fail"
    assert any("shell contains unsafe asset reference" in error for error in check.errors)
    assert external_ref not in calls
    assert "external-secret-value" not in serialized
    assert "X-Amz-Signature" not in serialized


def test_check_route_duplicate_src_uses_browser_first_value_without_leak() -> None:
    external_ref = "https://cdn.example/external-secret.js"
    shell_body = (
        b'<link rel="stylesheet" href="/dev/assets/index.css">'
        + (f'<script src="{external_ref}" src="/dev/assets/index.js"></script>').encode()
    )
    responses = _mock_dev_route_responses(shell_body=shell_body)
    calls: list[str] = []

    def fetcher(
        url: str,
        *,
        timeout: float,
        method: str,
        follow_redirects: bool,
    ) -> HttpResponse:
        assert timeout == 3.0
        calls.append(url)
        return responses[url]

    check = check_route(
        route_url="https://yylx.world/dev",
        expected_environment="staging",
        expected_api_base="https://yylx.world/dev/api",
        timeout=3.0,
        fetcher=fetcher,
    )

    serialized = json.dumps(asdict(check))
    assert check.status == "fail"
    assert any("shell contains unsafe asset reference" in error for error in check.errors)
    assert all("external-secret" not in url for url in calls)
    assert "https://yylx.world/dev/assets/index.js" not in calls
    assert "external-secret" not in serialized


@pytest.mark.parametrize(
    ("duplicate_element", "untrusted_marker", "unexpected_fetch"),
    [
        (
            '<link rel="stylesheet" href="/dev/assets/duplicate.css" '
            'href="https://cdn.example/second-secret.css">',
            "second-secret",
            "https://yylx.world/dev/assets/duplicate.css",
        ),
        (
            '<link rel="preload" rel="stylesheet" href="https://cdn.example/rel-secret.css">',
            "rel-secret",
            "https://cdn.example/rel-secret.css",
        ),
    ],
)
def test_check_route_rejects_duplicate_link_security_attributes_without_leak(
    duplicate_element: str,
    untrusted_marker: str,
    unexpected_fetch: str,
) -> None:
    shell_body = (
        b'<link rel="stylesheet" href="/dev/assets/index.css">' + duplicate_element.encode()
    )
    responses = _mock_dev_route_responses(shell_body=shell_body)
    calls: list[str] = []

    def fetcher(
        url: str,
        *,
        timeout: float,
        method: str,
        follow_redirects: bool,
    ) -> HttpResponse:
        assert timeout == 3.0
        calls.append(url)
        return responses[url]

    check = check_route(
        route_url="https://yylx.world/dev",
        expected_environment="staging",
        expected_api_base="https://yylx.world/dev/api",
        timeout=3.0,
        fetcher=fetcher,
    )

    serialized = json.dumps(asdict(check))
    assert check.status == "fail"
    assert any("shell contains unsafe asset reference" in error for error in check.errors)
    assert unexpected_fetch not in calls
    assert all(untrusted_marker not in url for url in calls)
    assert untrusted_marker not in serialized


@pytest.mark.parametrize(
    "unsafe_ref",
    [
        "https:///dev/assets/raw-secret.js",
        "//cdn.example/dev/assets/raw-secret.js",
        r"/dev\assets\raw-secret.js",
        "/dev//assets/raw-secret.js",
        "https://yylx.world//dev/assets/raw-secret.js",
        "https://yylx.world/dev/assets/raw-secret.js",
        "/dev/assets/raw-secret.js?",
        "/dev/assets/raw-secret.js#",
    ],
)
def test_check_route_rejects_ambiguous_raw_asset_syntax_without_fetch_or_leak(
    unsafe_ref: str,
) -> None:
    stylesheet_url = "https://yylx.world/dev/assets/index.css"
    shell_body = (
        b'<link rel="stylesheet" href="/dev/assets/index.css">'
        + f'<script src="{unsafe_ref}"></script>'.encode()
    )
    responses = _mock_dev_route_responses(shell_body=shell_body)
    calls: list[str] = []

    def fetcher(
        url: str,
        *,
        timeout: float,
        method: str,
        follow_redirects: bool,
    ) -> HttpResponse:
        assert timeout == 3.0
        calls.append(url)
        return responses[url]

    check = check_route(
        route_url="https://yylx.world/dev",
        expected_environment="staging",
        expected_api_base="https://yylx.world/dev/api",
        timeout=3.0,
        fetcher=fetcher,
    )

    serialized = json.dumps(asdict(check))
    assert check.status == "fail"
    assert any(
        marker in error
        for marker in (
            "shell contains unsafe asset reference",
            "shell contains malformed asset reference",
        )
        for error in check.errors
    )
    assert stylesheet_url in calls
    assert all("raw-secret" not in url for url in calls)
    assert "raw-secret" not in serialized


@pytest.mark.parametrize(
    "malformed_ref",
    [
        "https://yylx.world:not-a-port/dev/assets/bad.js",
        "https://[invalid-ipv6/dev/assets/bad.js",
    ],
)
def test_check_route_bounds_malformed_asset_ref_and_continues(
    malformed_ref: str,
) -> None:
    stylesheet_url = "https://yylx.world/dev/assets/index.css"
    shell_body = (
        b'<link rel="stylesheet" href="/dev/assets/index.css">'
        + f'<script src="{malformed_ref}"></script>'.encode()
    )
    responses = _mock_dev_route_responses(shell_body=shell_body)
    calls: list[str] = []

    def fetcher(
        url: str,
        *,
        timeout: float,
        method: str,
        follow_redirects: bool,
    ) -> HttpResponse:
        assert timeout == 3.0
        calls.append(url)
        return responses[url]

    check = check_route(
        route_url="https://yylx.world/dev",
        expected_environment="staging",
        expected_api_base="https://yylx.world/dev/api",
        timeout=3.0,
        fetcher=fetcher,
    )

    serialized = json.dumps(asdict(check))
    assert check.status == "fail"
    assert any("shell contains malformed asset reference" in error for error in check.errors)
    assert stylesheet_url in calls
    assert malformed_ref not in serialized


def test_check_route_reports_malformed_asset_reference_without_crashing() -> None:
    canonical_url = "https://yylx.world/dev/"
    responses = _mock_dev_route_responses()
    responses[canonical_url] = HttpResponse(
        url=canonical_url,
        status=200,
        headers={"content-type": "text/html"},
        body=b'<script type="module" src="/dev/assets/\xff.js"></script>',
    )

    def fetcher(
        url: str,
        *,
        timeout: float,
        method: str,
        follow_redirects: bool,
    ) -> HttpResponse:
        assert timeout == 3.0
        return responses[url]

    check = check_route(
        route_url="https://yylx.world/dev",
        expected_environment="staging",
        expected_api_base="https://yylx.world/dev/api",
        timeout=3.0,
        fetcher=fetcher,
    )

    assert check.status == "fail"
    assert "https://yylx.world/dev/: shell contains malformed asset reference" in check.errors


@pytest.mark.parametrize(
    "route_url",
    [
        "https://operator:yylx-secret@yylx.world/dev",
        "https://yylx.world/dev?debug",
        "https://yylx.world/dev#private-fragment",
    ],
)
def test_parse_route_rejects_unsafe_route_url_components(route_url: str) -> None:
    with pytest.raises(
        argparse.ArgumentTypeError,
        match="route URL must not contain userinfo, query, or fragment",
    ):
        _parse_route(
            f"staging={route_url}=https://yylx.world/dev/api",
        )


@pytest.mark.parametrize(
    "expected_api_base",
    [
        "https://operator:yylx-secret@yylx.world/dev/api",
        "https://yylx.world/dev/api?debug",
        "https://yylx.world/dev/api#private-fragment",
    ],
)
def test_parse_route_rejects_unsafe_expected_api_base(
    expected_api_base: str,
) -> None:
    with pytest.raises(
        argparse.ArgumentTypeError,
        match="expected API base must not contain userinfo, query, or fragment",
    ):
        _parse_route(
            f"staging=https://yylx.world/dev={expected_api_base}",
        )


@pytest.mark.parametrize(
    ("route_url", "expected_api_base", "error"),
    [
        (
            "https://[invalid-ipv6/dev",
            "https://yylx.world/dev/api",
            "route URL is malformed",
        ),
        (
            "https://yylx.world:not-a-port/dev",
            "https://yylx.world/dev/api",
            "route URL is malformed",
        ),
        (
            "https://yylx.world/dev",
            "https://[invalid-ipv6/dev/api",
            "expected API base is malformed",
        ),
        (
            "https://yylx.world/dev",
            "https://yylx.world:not-a-port/dev/api",
            "expected API base is malformed",
        ),
    ],
)
def test_parse_route_bounds_malformed_route_and_api_urls(
    route_url: str,
    expected_api_base: str,
    error: str,
) -> None:
    with pytest.raises(argparse.ArgumentTypeError, match=error):
        _parse_route(f"staging={route_url}={expected_api_base}")


def test_web_runtime_config_script_writes_public_metadata(tmp_path: Path) -> None:
    document, _ = _run_runtime_config(
        tmp_path,
        environment="production",
        route_path="/prod",
    )
    assert document == {
        "environment": "production",
        "environmentLabel": "Production",
        "routePath": "/prod",
        "apiBase": "/prod",
        "apiRouteBase": "https://yylx.world/prod/api",
        "buildRevision": "unknown",
        "sourceRef": "unknown",
        "buildKind": "unknown",
        "sourceDigest": "unknown",
        "sourceBaseCommit": "unknown",
        "buildTime": "",
    }


def test_web_runtime_config_prefixes_staging_assets(tmp_path: Path) -> None:
    _, html = _run_runtime_config(tmp_path, environment="staging", route_path="/staging")
    assert 'href="/staging/assets/index.css"' in html
    assert 'src="/staging/assets/index.js"' in html
    assert '="./assets/' not in html


def test_web_runtime_config_anchors_root_assets_for_deep_routes(tmp_path: Path) -> None:
    _, html = _run_runtime_config(tmp_path, environment="local", route_path="")
    assert 'href="/assets/index.css"' in html
    assert 'src="/assets/index.js"' in html
    assert urljoin("https://yylx.world/auth/reset", "/assets/index.js") == (
        "https://yylx.world/assets/index.js"
    )


def test_web_runtime_config_restart_never_retains_or_doubles_prefix(
    tmp_path: Path,
) -> None:
    _run_runtime_config(tmp_path, environment="staging", route_path="/staging")
    _, prod_html = _run_runtime_config(
        tmp_path,
        environment="production",
        route_path="/prod",
    )
    assert prod_html.count("/prod/assets/") == 2
    assert "/staging/assets/" not in prod_html
    assert "/prod/prod/assets/" not in prod_html

    _, html = _run_runtime_config(tmp_path, environment="staging", route_path="/staging")
    assert html.count("/staging/assets/") == 2
    assert "/staging/staging/assets/" not in html
    assert "/prod/assets/" not in html

    _, root_html = _run_runtime_config(tmp_path, environment="local", route_path="")
    assert root_html.count("/assets/") == 2
    assert "/staging/assets/" not in root_html
