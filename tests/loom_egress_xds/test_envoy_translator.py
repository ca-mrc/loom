"""envoy_translator: Snapshot → CDS+RDS protobuf (#190 PR-C1b).

Tests assert the protobuf shape directly rather than serializing +
parsing — this catches Envoy-side schema drift if/when we bump
envoy-data-plane. The Envoy spike (#196) validated that this shape
actually works end-to-end with Envoy v1.30.
"""

from __future__ import annotations

import pytest

# Import skip: envoy-data-plane / xds-protos belong to the standalone egress
# control plane and are NOT in the default dev install. Its Dockerfile installs
# them; local devs run `uv pip install envoy-data-plane xds-protos`
# (or skip this test).
envoy_data_plane = pytest.importorskip("envoy.config.cluster.v3.cluster_pb2")


from dataclasses import dataclass, field  # noqa: E402
from datetime import datetime  # noqa: E402
from uuid import UUID  # noqa: E402

from envoy.config.cluster.v3.cluster_pb2 import Cluster  # noqa: E402

from loom_egress_xds.config_builder import build_snapshot  # noqa: E402
from loom_egress_xds.envoy_translator import (  # noqa: E402
    CONNECTION_ID_HEADER,
    ROUTE_CONFIGURATION_NAME,
    build_envoy_config,
)


@dataclass
class _Row:
    id: UUID
    resolved_egress_ips: list[str] = field(default_factory=list)
    upstream_host: str = "api.openai.com"
    base_url: str = "https://api.openai.com/v1"
    deleted_at: datetime | None = None


_C1 = UUID("00000000-0000-0000-0000-000000000001")
_C2 = UUID("00000000-0000-0000-0000-000000000002")


def test_empty_snapshot_yields_zero_clusters_one_routeconfig() -> None:
    snap = build_snapshot([])
    cfg = build_envoy_config(snap)
    assert cfg.clusters == ()
    # Always one RouteConfiguration even when empty; Envoy needs SOME
    # RDS response to consume, otherwise it hangs at startup.
    assert cfg.route_configuration.name == ROUTE_CONFIGURATION_NAME
    assert len(cfg.route_configuration.virtual_hosts) == 1
    assert list(cfg.route_configuration.virtual_hosts[0].routes) == []
    assert cfg.version_info == snap.version


def test_single_connection_yields_one_cluster_one_route() -> None:
    rows = [
        _Row(
            id=_C1,
            resolved_egress_ips=["1.2.3.4", "5.6.7.8"],
            upstream_host="api.openai.com",
        )
    ]
    snap = build_snapshot(rows)
    cfg = build_envoy_config(snap)

    # Cluster: name + endpoints
    assert len(cfg.clusters) == 1
    cluster = cfg.clusters[0]
    assert cluster.name == "egress-00000000-0000-0000-0000-000000000001"
    assert cluster.type == Cluster.DiscoveryType.STATIC
    assert cluster.lb_policy == Cluster.LbPolicy.ROUND_ROBIN

    # Endpoints are the allowlisted IPs (sorted by build_snapshot).
    endpoints = cluster.load_assignment.endpoints[0].lb_endpoints
    addrs = [e.endpoint.address.socket_address.address for e in endpoints]
    ports = [e.endpoint.address.socket_address.port_value for e in endpoints]
    assert addrs == ["1.2.3.4", "5.6.7.8"]
    assert ports == [443, 443]

    # Route: pair match on header + :authority
    assert len(cfg.route_configuration.virtual_hosts) == 1
    routes = cfg.route_configuration.virtual_hosts[0].routes
    assert len(routes) == 1
    route = routes[0]

    # ConnectMatcher is present (route applies to CONNECT requests).
    assert route.match.HasField("connect_matcher")

    # TWO header matchers: x-loom-connection-id AND :authority.
    headers = list(route.match.headers)
    assert len(headers) == 2
    by_name = {h.name: h for h in headers}
    assert CONNECTION_ID_HEADER in by_name
    assert ":authority" in by_name
    assert by_name[CONNECTION_ID_HEADER].string_match.exact == (
        "00000000-0000-0000-0000-000000000001"
    )
    assert by_name[":authority"].string_match.exact == "api.openai.com:443"

    # Route action: dispatch to the per-connection cluster.
    assert route.route.cluster == ("egress-00000000-0000-0000-0000-000000000001")

    # CONNECT upgrade config so Envoy terminates the CONNECT here.
    assert len(route.route.upgrade_configs) == 1
    assert route.route.upgrade_configs[0].upgrade_type == "CONNECT"
    assert route.route.upgrade_configs[0].HasField("connect_config")


def test_multiple_connections_produce_clusters_in_order() -> None:
    rows = [
        _Row(id=_C2, resolved_egress_ips=["9.9.9.9"], upstream_host="b.com"),
        _Row(id=_C1, resolved_egress_ips=["1.1.1.1"], upstream_host="a.com"),
    ]
    snap = build_snapshot(rows)
    cfg = build_envoy_config(snap)
    # build_snapshot sorts by connection_id; translator preserves
    # that order so Envoy never sees a churned cluster list for the
    # same logical data.
    cluster_names = [c.name for c in cfg.clusters]
    assert cluster_names == [
        "egress-00000000-0000-0000-0000-000000000001",
        "egress-00000000-0000-0000-0000-000000000002",
    ]
    route_clusters = [r.route.cluster for r in cfg.route_configuration.virtual_hosts[0].routes]
    assert route_clusters == [
        "egress-00000000-0000-0000-0000-000000000001",
        "egress-00000000-0000-0000-0000-000000000002",
    ]


def test_version_info_passthrough() -> None:
    # version_info MUST be the snapshot version verbatim so the
    # xds_server's snapshot cache can correlate watcher pushes with
    # DiscoveryResponses.
    snap = build_snapshot([_Row(id=_C1, resolved_egress_ips=["1.2.3.4"])])
    cfg = build_envoy_config(snap)
    assert cfg.version_info == snap.version
    assert len(cfg.version_info) == 16  # sha256-prefix from config_builder


def test_upstream_host_drives_authority_match() -> None:
    rows = [
        _Row(
            id=_C1,
            resolved_egress_ips=["1.1.1.1"],
            upstream_host="custom.host.example.com",
        )
    ]
    snap = build_snapshot(rows)
    cfg = build_envoy_config(snap)
    routes = cfg.route_configuration.virtual_hosts[0].routes
    authority_match = next(h for h in routes[0].match.headers if h.name == ":authority")
    assert authority_match.string_match.exact == ("custom.host.example.com:443")


def test_https_cluster_endpoint_port_defaults_to_443() -> None:
    rows = [_Row(id=_C1, resolved_egress_ips=["1.1.1.1"])]
    snap = build_snapshot(rows)
    cfg = build_envoy_config(snap)
    port = (
        cfg.clusters[0]
        .load_assignment.endpoints[0]
        .lb_endpoints[0]
        .endpoint.address.socket_address.port_value
    )
    assert port == 443


def test_http_non443_provider_uses_plain_http_route_and_configured_port() -> None:
    rows = [
        _Row(
            id=_C1,
            resolved_egress_ips=["192.168.32.1"],
            upstream_host="192.168.32.1",
            base_url="http://192.168.32.1:28001/v1",
        )
    ]
    snap = build_snapshot(rows)
    cfg = build_envoy_config(snap)

    endpoint = (
        cfg.clusters[0].load_assignment.endpoints[0].lb_endpoints[0].endpoint.address.socket_address
    )
    assert endpoint.address == "192.168.32.1"
    assert endpoint.port_value == 28001

    route = cfg.route_configuration.virtual_hosts[0].routes[0]
    assert not route.match.HasField("connect_matcher")
    assert route.match.prefix == "/"
    by_name = {h.name: h for h in route.match.headers}
    assert by_name[CONNECTION_ID_HEADER].string_match.exact == (
        "00000000-0000-0000-0000-000000000001"
    )
    assert by_name[":authority"].string_match.exact == "192.168.32.1:28001"
    assert list(route.request_headers_to_remove) == [CONNECTION_ID_HEADER]
    assert list(route.route.upgrade_configs) == []


def test_deleted_rows_propagate_through_build_snapshot() -> None:
    # build_snapshot excludes soft-deleted rows; translator just
    # iterates entries, so deleted rows naturally don't produce
    # clusters or routes. Test it end-to-end so the contract is
    # documented at the translator layer too.
    rows = [
        _Row(id=_C1, resolved_egress_ips=["1.1.1.1"]),
        _Row(
            id=_C2,
            resolved_egress_ips=["2.2.2.2"],
            deleted_at=datetime.fromisoformat("2026-06-18T00:00:00+00:00"),
        ),
    ]
    snap = build_snapshot(rows)
    cfg = build_envoy_config(snap)
    assert len(cfg.clusters) == 1
    assert cfg.clusters[0].name == ("egress-00000000-0000-0000-0000-000000000001")


@pytest.mark.parametrize("base_url", ["https://api.openai.com/v1", "http://api.openai.com:28001/v1"])
def test_provider_cluster_uses_protocol_maximum_active_thresholds(base_url: str) -> None:
    cfg = build_envoy_config(build_snapshot([
        _Row(id=_C1, resolved_egress_ips=["192.0.2.1"], base_url=base_url),
    ]))
    # These provider routes use DEFAULT priority. Leaving it unspecified gives
    # Envoy 1.30 a 1024-connection/request/pending ceiling per credential CID.
    assert all(route.route.priority == 0 for route in cfg.route_configuration.virtual_hosts[0].routes)
    thresholds = list(cfg.clusters[0].circuit_breakers.thresholds)
    assert len(thresholds) == 1
    threshold = thresholds[0]
    assert threshold.priority == 0
    for name in ("max_connections", "max_requests", "max_pending_requests"):
        assert threshold.HasField(name)
        assert getattr(threshold, name).value == (1 << 32) - 1
    assert not threshold.HasField("max_retries")
    assert not threshold.HasField("retry_budget")
    assert list(cfg.clusters[0].circuit_breakers.per_host_thresholds) == []
