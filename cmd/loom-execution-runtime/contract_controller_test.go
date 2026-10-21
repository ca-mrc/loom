package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func isolatedControllerPlan() plan {
	p := testPlan("/workspace", phase{Role: "agent", Argv: []string{"/bin/true"}, WorkingDirectory: "/app", TimeoutSeconds: 60})
	p.AgentImageRef = &p.TaskImageRef
	p.VerifierExecution = "in_attempt"
	p.Verifier = &phase{Role: "verifier", Argv: []string{"/bin/true"}, WorkingDirectory: "/app", TimeoutSeconds: 60}
	p.TaskResources = resources{CPUMillis: 8000, MemoryMiB: 16384, EphemeralStorageMiB: 10240}
	p.ControllerResources = &resources{CPUMillis: 1000, MemoryMiB: 2048, EphemeralStorageMiB: 10240}
	ready := probe{Kind: "exec", Argv: []string{"/bin/true"}, TimeoutSeconds: 2, PeriodSeconds: 2, FailureThreshold: 30}
	for _, name := range []string{"task-sandbox", "verifier-sandbox"} {
		p.Sidecars = append(p.Sidecars, sidecar{RoleName: name, ImageRef: p.TaskImageRef, Argv: []string{"/bin/true"}, Resources: p.TaskResources, StartupProbe: ready, ReadinessProbe: ready, PrivateSandbox: true})
	}
	return p
}

func TestControllerResourcesRoundTripAndLegacyPlan(t *testing.T) {
	for _, configured := range []bool{false, true} {
		p := isolatedControllerPlan()
		if !configured {
			p.ControllerResources = nil
		}
		body, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(body), "controller_resources") != configured {
			t.Fatalf("legacy plan serialization changed: %s", body)
		}
		path := filepath.Join(t.TempDir(), "plan.json")
		if err := os.WriteFile(path, body, 0600); err != nil {
			t.Fatal(err)
		}
		loaded, err := loadPlan(path)
		if err != nil {
			t.Fatal(err)
		}
		if loaded.TaskResources != p.TaskResources || (loaded.ControllerResources != nil) != configured {
			t.Fatal("resources changed during strict plan loading")
		}
		if configured && *loaded.ControllerResources != *p.ControllerResources {
			t.Fatal("controller resources changed during strict plan loading")
		}
	}
}

func TestControllerSizingPreservesTaskBoundary(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*plan)
		reason string
	}{
		{"missing controller", func(p *plan) { p.AgentImageRef = nil }, "agent image reference"},
		{"missing verifier", func(p *plan) { p.Sidecars = p.Sidecars[:1] }, "isolated attempt controller"},
		{"smaller sandbox", func(p *plan) { p.Sidecars[0].Resources.CPUMillis = 1 }, "preserve task and verifier"},
		{"smaller storage", func(p *plan) { p.ControllerResources.EphemeralStorageMiB = 1 }, "preserve task-derived storage"},
		{"invalid compute", func(p *plan) { p.ControllerResources.MemoryMiB = 0 }, "invalid controller resources"},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			p := isolatedControllerPlan()
			item.mutate(&p)
			if err := p.validate(); err == nil || !strings.Contains(err.Error(), item.reason) {
				t.Fatalf("wanted %s, got %v", item.reason, err)
			}
		})
	}
}
