package main

import (
	"fmt"
	"sort"
	"strings"
)

// mainlandProxyProfile is frozen into the authenticated runtime contract. It
// executes the same domain/suffix rules as the Docker mihomo gateway; it never
// fetches or changes a rule list while an attempt is running.
type mainlandProxyProfile struct {
	SchemaVersion      string         `json:"schema_version"`
	ProviderVersion    string         `json:"provider_version"`
	GFWSuffixes        []string       `json:"gfw_suffixes"`
	GFWHosts           []string       `json:"gfw_hosts"`
	GreatFireHosts     []string       `json:"greatfire_hosts"`
	UnknownAction      string         `json:"unknown_action"`
	IntermittentAction string         `json:"intermittent_action"`
	Sources            map[string]any `json:"sources"`
}

func (p *mainlandProxyProfile) validate() error {
	if p == nil || p.SchemaVersion != "loom.mainland-network-profile.v1" ||
		len(p.ProviderVersion) < 1 || len(p.ProviderVersion) > 128 ||
		p.UnknownAction != "direct" || p.IntermittentAction != "reject" ||
		len(p.GFWSuffixes)+len(p.GFWHosts) == 0 {
		return fmt.Errorf("invalid mainland proxy profile")
	}
	for _, hosts := range [][]string{p.GFWSuffixes, p.GFWHosts, p.GreatFireHosts} {
		if len(hosts) > 200000 || !sort.StringsAreSorted(hosts) {
			return fmt.Errorf("mainland proxy rules must be canonical, unique and sorted")
		}
		previous := ""
		for _, host := range hosts {
			if host == previous || !(webDestination{Host: host, Protocol: "https"}).valid() {
				return fmt.Errorf("mainland proxy rules must be canonical, unique and sorted")
			}
			previous = host
		}
	}
	return nil
}

func hasMainlandHost(hosts []string, host string) bool {
	index := sort.SearchStrings(hosts, host)
	return index < len(hosts) && hosts[index] == host
}

func (p *mainlandProxyProfile) denialReason(d webDestination) string {
	if p == nil {
		return ""
	}
	if hasMainlandHost(p.GreatFireHosts, d.Host) {
		return "mainland_proxy_greatfire_denied"
	}
	if hasMainlandHost(p.GFWHosts, d.Host) {
		return "mainland_proxy_gfw_denied"
	}
	for host := d.Host; host != ""; {
		if hasMainlandHost(p.GFWSuffixes, host) {
			return "mainland_proxy_gfw_denied"
		}
		_, tail, found := strings.Cut(host, ".")
		if !found {
			break
		}
		host = tail
	}
	return ""
}

func (p phasedEgressPolicy) denialReason(d webDestination) string {
	if p.broker.currentPhaseRole() == "setup" {
		return ""
	}
	return p.mainland.denialReason(d)
}
