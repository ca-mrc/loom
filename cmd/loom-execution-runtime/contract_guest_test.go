package main

import (
	"encoding/json"
	"testing"
)

func guestPlanPayload(t *testing.T) map[string]any {
	t.Helper()
	p := preparedTaskPlan()
	p.RuntimeVolumeMiB = 1024
	p.ExecutionClassID = "linux-amd64-cpu-guest-v1"
	p.VerifierExecution = "in_attempt"
	verifier := p.Main
	verifier.Role = "verifier"
	p.Verifier = &verifier
	p.TaskResources = resources{CPUMillis: 1000, MemoryMiB: 1024, EphemeralStorageMiB: 2048}
	controller := p.TaskResources
	controller.EphemeralStorageMiB += p.RuntimeVolumeMiB
	p.ControllerResources = &controller
	for i := range p.Sidecars {
		s := &p.Sidecars[i]
		socket := "/loom/sandboxes/" + s.RoleName + "/sandbox.sock"
		s.Argv = []string{"/loom/bin/loom-sandbox-runtime", "--socket", socket, "--exec-timeout-seconds", "60"}
		s.StartupProbe.Argv = []string{"/loom/bin/loom-sandbox-runtime", "--check-socket", socket}
		s.ReadinessProbe.Argv = append([]string(nil), s.StartupProbe.Argv...)
		s.Resources = p.TaskResources
		zero := int64(0)
		s.Identity = &sandboxIdentity{RunAsUser: &zero, RunAsGroup: &zero, Home: "/root"}
	}
	raw, _ := json.Marshal(p)
	var result map[string]any
	if err := json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	for _, item := range result["sidecars"].([]any) {
		item.(map[string]any)["guest_execution"] = map[string]any{
			"schema_version": "loom.guest-execution.v1", "runtime": "qemu-tcg-v1",
			"capabilities": []any{"nested_docker", "singularity_mounts"},
		}
	}
	return result
}

func TestGuestPlanStrictRoundTripAndOrdinaryOmission(t *testing.T) {
	raw, _ := json.Marshal(guestPlanPayload(t))
	p, err := decodePlan(raw)
	if err != nil {
		t.Fatal(err)
	}
	roundtrip, _ := json.Marshal(p)
	var got map[string]any
	_ = json.Unmarshal(roundtrip, &got)
	if got["sidecars"].([]any)[0].(map[string]any)["guest_execution"] == nil {
		t.Fatal("guest authority lost")
	}
	p = preparedTaskPlan()
	raw, _ = json.Marshal(p)
	_ = json.Unmarshal(raw, &got)
	if _, present := got["sidecars"].([]any)[0].(map[string]any)["guest_execution"]; present {
		t.Fatal("legacy bytes changed")
	}
}

func TestPlainGuestWithoutDeclaredCapabilitiesIsAccepted(t *testing.T) {
	p := guestPlanPayload(t)
	for _, item := range p["sidecars"].([]any) {
		item.(map[string]any)["guest_execution"].(map[string]any)["capabilities"] = []any{}
	}
	raw, _ := json.Marshal(p)
	if _, err := decodePlan(raw); err != nil {
		t.Fatal(err)
	}
}

func TestGuestPlanRejectsPartialOrUnsafeAuthority(t *testing.T) {
	for _, damage := range []string{"ordinary_class", "missing_guest", "one_guest", "empty_caps", "missing_caps", "unknown_cap", "different_caps", "duplicate_caps", "unsorted_caps", "nonroot", "short_volume", "wrong_socket", "wrong_probe", "small_memory", "small_storage", "small_cpu", "bad_timeout", "foreign_sidecar", "wrong_schema", "wrong_runtime", "missing_controller", "storage_unreserved", "small_request", "wrong_image", "skipped_verifier", "separate_verifier"} {
		t.Run(damage, func(t *testing.T) {
			p := guestPlanPayload(t)
			sides := p["sidecars"].([]any)
			s := sides[0].(map[string]any)
			g := s["guest_execution"].(map[string]any)
			switch damage {
			case "skipped_verifier":
				p["verifier_execution"] = "skipped"
				p["verifier"] = nil
			case "separate_verifier":
				p["verifier_execution"] = "separate_execution"
				p["verifier"] = nil
			case "missing_controller":
				delete(p, "controller_resources")
			case "storage_unreserved":
				p["controller_resources"].(map[string]any)["ephemeral_storage_mib"] = 2048
			case "small_request":
				p["resource_requests"] = map[string]any{"controller": map[string]any{"cpu_millis": 100, "memory_mib": 128, "ephemeral_storage_mib": 1024}}
			case "wrong_image":
				s["image_ref"] = p["agent_image_ref"]
			case "ordinary_class":
				p["execution_class_id"] = "linux-amd64-cpu-pod-v1"
			case "missing_guest":
				for _, x := range sides {
					delete(x.(map[string]any), "guest_execution")
				}
			case "one_guest":
				delete(s, "guest_execution")
			case "empty_caps":
				g["capabilities"] = []any{}
			case "missing_caps":
				for _, x := range sides {
					delete(x.(map[string]any)["guest_execution"].(map[string]any), "capabilities")
				}
			case "unknown_cap":
				g["capabilities"] = []any{"external_cluster"}
			case "different_caps":
				g["capabilities"] = []any{"nested_docker"}
			case "duplicate_caps":
				g["capabilities"] = []any{"nested_docker", "nested_docker"}
			case "unsorted_caps":
				g["capabilities"] = []any{"singularity_mounts", "nested_docker"}
			case "nonroot":
				s["identity"].(map[string]any)["run_as_user"] = 65532
			case "short_volume":
				p["runtime_volume_mib"] = 1023
			case "wrong_socket":
				s["argv"].([]any)[2] = "/tmp/foreign.sock"
			case "wrong_probe":
				s["startup_probe"].(map[string]any)["argv"].([]any)[2] = "/tmp/foreign.sock"
			case "small_memory":
				s["resources"].(map[string]any)["memory_mib"] = 511
			case "small_storage":
				s["resources"].(map[string]any)["ephemeral_storage_mib"] = 159
			case "small_cpu":
				s["resources"].(map[string]any)["cpu_millis"] = 999
			case "bad_timeout":
				s["argv"].([]any)[4] = "86401"
			case "foreign_sidecar":
				p["sidecars"] = append(sides, sides[0])
			case "wrong_schema":
				g["schema_version"] = "future"
			case "wrong_runtime":
				g["runtime"] = "host-docker"
			}
			raw, _ := json.Marshal(p)
			if _, err := decodePlan(raw); err == nil {
				t.Fatal("unsafe guest launch accepted")
			}
		})
	}
}

func TestEmulatedAuthenticationRequiresExactGuestClass(t *testing.T) {
	for _, tc := range []struct {
		class, capability string
		allowed           bool
	}{
		{"linux-amd64-cpu-guest-auth-v1", "emulated_pkcs11_authentication", true},
		{"linux-amd64-cpu-guest-auth-web-v1", "emulated_pkcs11_authentication", true},
		{"linux-amd64-cpu-guest-v1", "emulated_pkcs11_authentication", false},
		{"linux-amd64-cpu-guest-web-v1", "emulated_pkcs11_authentication", false},
		{"linux-amd64-cpu-guest-auth-v1", "pkcs11_authentication", false},
		{"linux-amd64-cpu-guest-auth-v1", "nested_docker", false},
	} {
		t.Run(tc.class+"/"+tc.capability, func(t *testing.T) {
			p := guestPlanPayload(t)
			p["execution_class_id"] = tc.class
			for _, sidecar := range p["sidecars"].([]any) {
				sidecar.(map[string]any)["guest_execution"].(map[string]any)["capabilities"] = []any{tc.capability}
			}
			raw, _ := json.Marshal(p)
			_, err := decodePlan(raw)
			if (err == nil) != tc.allowed {
				t.Fatalf("allowed=%v, error=%v", tc.allowed, err)
			}
		})
	}
}
