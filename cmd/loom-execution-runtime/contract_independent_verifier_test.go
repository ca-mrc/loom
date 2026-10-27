package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIndependentVerifierComponentKeepsSeparateResources(t *testing.T) {
	p := preparedTaskPlan()
	p.ControllerResources = &resources{CPUMillis: 1000, MemoryMiB: 2048, EphemeralStorageMiB: p.TaskResources.EphemeralStorageMiB}
	component := "verifier"
	p.Sidecars[1].TaskImageComponent = &component
	p.Sidecars[1].ImageRef = "registry/grading@sha256:" + strings.Repeat("c", 64)
	p.Sidecars[1].Resources = resources{CPUMillis: 3000, MemoryMiB: 6144, EphemeralStorageMiB: 12288}
	p.ResourceRequests = &executionResourceRequests{VerifierSandbox: &p.Sidecars[1].Resources}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "plan.json")
	if err = os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadPlan(path)
	if err != nil {
		t.Fatalf("distinct prepared verifier rejected: %v", err)
	}
	if loaded.Sidecars[1].Resources != p.Sidecars[1].Resources || loaded.Sidecars[1].ImageRef != p.Sidecars[1].ImageRef {
		t.Fatal("verifier requirements changed")
	}
	p.Sidecars[0].TaskImageComponent = &component
	if err = p.validate(); err == nil {
		t.Fatal("verifier component accepted on agent role")
	}
}

func TestGuestAgentCanDeferIndependentVerifier(t *testing.T) {
	payload := guestPlanPayload(t)
	payload["sidecars"] = payload["sidecars"].([]any)[:1]
	payload["verifier_execution"] = "separate_execution"
	delete(payload, "verifier")
	payload["verifier_after_agent_timeout"] = false
	raw, _ := json.Marshal(payload)
	if _, err := decodePlan(raw); err != nil {
		t.Fatalf("deferred guest agent rejected: %v", err)
	}
	payload["verifier_after_agent_timeout"] = true
	raw, _ = json.Marshal(payload)
	if _, err := decodePlan(raw); err == nil {
		t.Fatal("in-pod verifier behavior accepted without resident verifier")
	}
}
