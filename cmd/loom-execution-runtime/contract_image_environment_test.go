package main

import (
	"strings"
	"testing"
)

func TestImagePublicFingerprintKeepsCredentialEnvironmentForbidden(t *testing.T) {
	for _, test := range []struct {
		name, value string
		valid       bool
	}{
		{"GPG_KEY", strings.Repeat("A", 40), true},
		{"GPG_KEY", strings.Repeat("b", 64), true},
		{"GPG_KEY", "private-value", false},
		{"API_KEY", strings.Repeat("A", 40), false},
		{"PROVIDER_TOKEN", "private-value", false},
	} {
		for _, target := range []string{"phase", "private"} {
			p := isolatedControllerPlan()
			if target == "phase" {
				p.Main.Environment = map[string]string{test.name: test.value}
			} else {
				p.Sidecars[0].Environment = map[string]string{test.name: test.value}
			}
			if err := p.validate(); (err == nil) != test.valid {
				t.Fatalf("%s %s valid=%v: %v", target, test.name, test.valid, err)
			}
		}
	}
}
