from loom.driver.network_policy import (
    compute_iptables_rules,
    render_iptables_commands,
)
from loom.models.networking import Allowlist, NoNetwork, Public


def test_public_has_no_block_rules():
    plan = compute_iptables_rules(Public())
    assert plan.flush is True
    assert plan.outbound_drops == []
    assert plan.allowed_domains == []
    assert plan.allowed_cidrs == []


def test_no_network_drops_default_outbound():
    plan = compute_iptables_rules(NoNetwork())
    assert plan.flush is True
    actions = [(r.chain, r.action) for r in plan.outbound_drops]
    assert ("OUTPUT", "DROP") in actions


def test_allowlist_records_domains_and_cidrs():
    plan = compute_iptables_rules(
        Allowlist(domains=("github.com", "pypi.org"), cidrs=("10.0.0.0/8",)),
    )
    assert plan.flush is True
    assert "github.com" in plan.allowed_domains
    assert "10.0.0.0/8" in plan.allowed_cidrs
    assert any(r.action == "DROP" for r in plan.outbound_drops)


def test_render_public_is_empty():
    """Public = no enforcement → no iptables commands, so vanilla images
    (without iptables installed) still work."""
    cmds = render_iptables_commands(compute_iptables_rules(Public()))
    assert cmds == []


def test_render_no_network_sets_default_drop():
    cmds = render_iptables_commands(compute_iptables_rules(NoNetwork()))
    assert any("iptables -P OUTPUT DROP" in c for c in cmds)


def test_render_allowlist_resolves_domains_at_apply_time():
    plan = compute_iptables_rules(Allowlist(domains=("example.com",), cidrs=("10.0.0.0/8",)))
    cmds = render_iptables_commands(plan)
    assert any("getent ahosts example.com" in c for c in cmds)
    # Resolved IPs pinned to /etc/hosts so subsequent connects skip DNS.
    assert any("/etc/hosts" in c for c in cmds)
    assert any("-d 10.0.0.0/8" in c for c in cmds)
    assert any("iptables -P OUTPUT DROP" in c for c in cmds)


# ──────────────────────────────────────────────────────────────────────
# Always-blocked CIDRs (#78 slice B): cloud metadata + link-local IPs
# get a DROP rule prepended whenever iptables is being applied at all,
# regardless of the operator-supplied policy.
# ──────────────────────────────────────────────────────────────────────


def test_render_allowlist_drops_metadata_ip_before_accepts():
    """The metadata-IP DROP MUST come before operator ACCEPTs in the
    OUTPUT chain — iptables matches top-down and stops at the first
    match, so a later ACCEPT for a wide CIDR wouldn't override an
    earlier DROP."""
    plan = compute_iptables_rules(
        Allowlist(
            domains=("loom-llm-gateway.loom",),
            # Deliberately broad CIDR that COULD cover 169.254.x.x;
            # the always-blocked DROP must still win.
            cidrs=("0.0.0.0/0",),
        ),
    )
    cmds = render_iptables_commands(plan)

    drop_idx = next(
        i for i, c in enumerate(cmds)
        if "169.254.169.254" in c and "DROP" in c
    )
    accept_cidr_idx = next(
        i for i, c in enumerate(cmds)
        if "-d 0.0.0.0/0" in c and "ACCEPT" in c
    )
    assert drop_idx < accept_cidr_idx, (
        f"metadata-IP DROP at {drop_idx} must precede wide ACCEPT at "
        f"{accept_cidr_idx}: {cmds}"
    )


def test_render_no_network_blocks_metadata_ip_explicitly():
    """NoNetwork already DROPs everything via default policy, but the
    explicit metadata DROP earlier in the chain makes the intent
    auditable + survives a chain-order edit later."""
    cmds = render_iptables_commands(
        compute_iptables_rules(NoNetwork()),
    )
    assert any(
        "169.254.169.254/32" in c and "DROP" in c for c in cmds
    )
    assert any(
        "169.254.0.0/16" in c and "DROP" in c for c in cmds
    )


def test_render_blocks_both_metadata_and_link_local_range():
    """Both the specific metadata IP and the broader link-local /16
    get DROPs. The /16 catches IMDSv2 hop-limit-bypass sibling IPs and
    arbitrary link-local services the host happens to expose."""
    cmds = render_iptables_commands(
        compute_iptables_rules(
            Allowlist(domains=("api.openai.com",)),
        ),
    )
    blocked_cidrs = [
        c for c in cmds
        if "iptables -A OUTPUT -d 169.254" in c and "DROP" in c
    ]
    assert len(blocked_cidrs) >= 2
    assert any("169.254.169.254/32" in c for c in blocked_cidrs)
    assert any("169.254.0.0/16" in c for c in blocked_cidrs)


def test_always_blocked_cidrs_appear_before_domain_accepts():
    """When an Allowlist lists domains, the resolved-IP ACCEPT comes
    later in the chain than the always-blocked DROPs."""
    plan = compute_iptables_rules(
        Allowlist(domains=("example.com",)),
    )
    cmds = render_iptables_commands(plan)

    drop_idx = next(
        i for i, c in enumerate(cmds)
        if "169.254.169.254" in c and "DROP" in c
    )
    domain_accept_idx = next(
        i for i, c in enumerate(cmds)
        if "getent ahosts example.com" in c
    )
    assert drop_idx < domain_accept_idx
