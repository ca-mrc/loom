import json

import pytest
from pydantic import TypeAdapter, ValidationError

from loom.models.networking import (
    Allowlist,
    GatewayOnly,
    NetworkPolicy,
    NoNetwork,
    Public,
    PublicWeb,
    UnsupportedNetworkPolicyOverrideError,
    WebAllowlist,
    WebDestination,
    resolve_effective_network_policy,
)


def test_public_serialises():
    p = Public()
    assert p.kind == "public"
    assert p.model_dump() == {"kind": "public"}


def test_no_network_serialises():
    p = NoNetwork()
    assert p.kind == "no-network"
    assert p.model_dump() == {"kind": "no-network"}


def test_allowlist_serialises():
    p = Allowlist(domains=("github.com", "pypi.org"), cidrs=("10.0.0.0/8",))
    assert p.kind == "allowlist"
    assert p.domains == ("github.com", "pypi.org")
    assert p.cidrs == ("10.0.0.0/8",)


def test_tagged_union_discriminates():
    adapter = TypeAdapter(NetworkPolicy)
    pub = adapter.validate_python({"kind": "public"})
    assert isinstance(pub, Public)
    none = adapter.validate_python({"kind": "no-network"})
    assert isinstance(none, NoNetwork)
    allow = adapter.validate_python(
        {"kind": "allowlist", "domains": ("a",), "cidrs": ("1.0.0.0/24",)},
    )
    assert isinstance(allow, Allowlist)
    assert allow.domains == ("a",)


_EVERY_POLICY = (
    Public(),
    NoNetwork(),
    GatewayOnly(),
    Allowlist(domains=("example.com",)),
    WebAllowlist(destinations=(WebDestination(host="example.com", protocol="https"),)),
    PublicWeb(),
)


@pytest.mark.parametrize("policy", _EVERY_POLICY, ids=lambda policy: policy.kind)
@pytest.mark.parametrize("exclude", ["exclude_defaults", "exclude_unset"])
def test_tag_survives_default_excluding_dumps(policy, exclude) -> None:
    adapter = TypeAdapter(NetworkPolicy)
    encoded = policy.model_dump_json(**{exclude: True})

    assert json.loads(encoded)["kind"] == policy.kind
    assert adapter.validate_json(encoded) == policy
    assert adapter.validate_python(policy.model_dump(**{exclude: True})) == policy


def test_tagged_union_rejects_unknown_kind():
    adapter = TypeAdapter(NetworkPolicy)
    with pytest.raises(ValidationError):
        adapter.validate_python({"kind": "bogus"})


def test_allowlist_requires_domains():
    with pytest.raises(ValidationError):
        Allowlist()  # type: ignore[call-arg]


def test_immutability():
    p = Allowlist(domains=("example.com",))
    with pytest.raises(ValidationError):
        p.domains = ("changed",)  # type: ignore[misc]


def test_override_selects_any_hosted_kind_regardless_of_supported_list() -> None:
    baseline = GatewayOnly()
    allowlist = WebAllowlist(destinations=(WebDestination(host="example.com", protocol="https"),))
    assert resolve_effective_network_policy(baseline=baseline, override=None) == baseline
    assert resolve_effective_network_policy(baseline=baseline, override=PublicWeb()) == PublicWeb()
    assert resolve_effective_network_policy(baseline=baseline, override=allowlist) == allowlist
    assert resolve_effective_network_policy(
        baseline=baseline, override=PublicWeb(), supported=frozenset({"gateway-only"}),
    ) == PublicWeb()


def test_override_rejects_legacy_kinds() -> None:
    with pytest.raises(UnsupportedNetworkPolicyOverrideError, match="hosted kind"):
        resolve_effective_network_policy(baseline=GatewayOnly(), override=Public())
    with pytest.raises(UnsupportedNetworkPolicyOverrideError, match="hosted kind"):
        resolve_effective_network_policy(baseline=GatewayOnly(), override=NoNetwork())
