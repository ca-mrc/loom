package main

import (
	"strings"
	"testing"
)

func TestControllerPublicFingerprintKeepsCredentialEnvironmentForbidden(t *testing.T) {
	for _, test := range []struct {
		name, value string
		valid       bool
	}{
		{"GPG_KEY", strings.Repeat("A", 40), true},
		{"GPG_KEY", strings.Repeat("b", 64), true},
		{"GPG_KEY", "private-value", false},
		{"API_KEY", strings.Repeat("A", 40), false},
		{"PROVIDER_TOKEN", "private-value", false},
		{"VNC_KEY_DELAY_MS", "35", true},
		{"VNC_KEY_DELAY_MS", "private-value", false},
	} {
		p := isolatedControllerPlan()
		p.Main.Environment = map[string]string{test.name: test.value}
		if err := p.validate(); (err == nil) != test.valid {
			t.Fatalf("controller %s valid=%v: %v", test.name, test.valid, err)
		}
	}
}

func TestPrivateSandboxAuthoredFixtureValuesKeepPlatformControlsReserved(t *testing.T) {
	for _, test := range []struct {
		name, value string
		valid       bool
	}{
		{"GPG_KEY", "authored-fixture-value", true},
		{"API_KEY", "authored-fixture-value", true},
		{"POSTGRES_PASSWORD", "authored-fixture-value", true},
		{"CUSTOMER_ADMIN_TOKEN", "authored-fixture-value", true},
		{"SEED_HMAC_SECRET", "authored-fixture-value", true},
		{"KUBECONFIG", "/fixture/config", false},
		{"HTTP_PROXY", "http://fixture:8080", false},
		{"LOOM_INTERNAL_TOKEN", "authored-fixture-value", false},
	} {
		p := isolatedControllerPlan()
		p.Sidecars[0].Environment = map[string]string{test.name: test.value}
		if err := p.validate(); (err == nil) != test.valid {
			t.Fatalf("private sandbox %s valid=%v: %v", test.name, test.valid, err)
		}
	}
}
