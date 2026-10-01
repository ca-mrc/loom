package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func preparedTaskPlan() plan {
	p := testPlan("/workspace", phase{
		Role: "agent", Argv: []string{"/bin/true"}, WorkingDirectory: "/workspace", TimeoutSeconds: 1,
	})
	id := "11111111-2222-3333-4444-555555555555"
	agentImage := "registry/agent@sha256:" + strings.Repeat("b", 64)
	p.TaskImageMaterializationID = &id
	p.AgentImageRef = &agentImage
	p.ImageAdmission.Admissions[0].Statement.ImageRef = agentImage
	ready := probe{Kind: "exec", Argv: []string{"/bin/true"}, TimeoutSeconds: 1, PeriodSeconds: 1, FailureThreshold: 1}
	p.Sidecars = []sidecar{{
		RoleName: "task-sandbox", PrivateSandbox: true, ImageRef: p.TaskImageRef,
		Argv: []string{"/bin/true"}, Resources: p.TaskResources, StartupProbe: ready, ReadinessProbe: ready,
	}}
	verifier := p.Sidecars[0]
	verifier.RoleName = "verifier-sandbox"
	p.Sidecars = append(p.Sidecars, verifier)
	return p
}

func TestDecodePlanSupportsPreparedImageAndLegacyOmission(t *testing.T) {
	prepared := preparedTaskPlan()
	legacy := testPlan("/workspace", prepared.Main)
	for name, p := range map[string]plan{"prepared": prepared, "legacy": legacy} {
		t.Run(name, func(t *testing.T) {
			payload, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(payload), "task_image_materialization_id") != (name == "prepared") {
				t.Fatalf("unexpected optional materialization field: %s", payload)
			}
			decoded, err := decodePlan(payload)
			if err != nil {
				t.Fatal(err)
			}
			if name == "prepared" && (decoded.TaskImageMaterializationID == nil || *decoded.TaskImageMaterializationID != *p.TaskImageMaterializationID) {
				t.Fatal("materialization identity was lost during decoding")
			}
		})
	}
}

func TestPreparedImageRequiresValidControllerBinding(t *testing.T) {
	for _, invalid := range []string{"", "not-a-uuid", "00000000-0000-0000-0000-000000000000", "11111111-2222-3333-4444-55555555555z"} {
		t.Run("uuid="+invalid, func(t *testing.T) {
			p := preparedTaskPlan()
			p.TaskImageMaterializationID = &invalid
			if err := p.validate(); err == nil || !strings.Contains(err.Error(), "materialization UUID") {
				t.Fatalf("invalid materialization UUID accepted: %v", err)
			}
		})
	}
	for name, mutate := range map[string]func(*plan){
		"missing agent": func(p *plan) { p.AgentImageRef = nil },
		"empty agent":   func(p *plan) { empty := ""; p.AgentImageRef = &empty },
		"precomposed":   func(p *plan) { p.Composition = "precomposed" },
	} {
		t.Run(name, func(t *testing.T) {
			p := preparedTaskPlan()
			mutate(&p)
			if err := p.validate(); err == nil {
				t.Fatal("invalid prepared-image controller binding accepted")
			}
		})
	}
}

func TestDeferredVerifierGradesOnTheAttemptsPreparedImage(t *testing.T) {
	p := preparedTaskPlan()
	p.ExecutionRole = "verifier"
	p.Main.Role = "verifier"
	p.VerifierExecution = "skipped"
	p.Verifier = nil
	p.Sidecars = p.Sidecars[1:]
	if err := p.validate(); err != nil {
		t.Fatalf("deferred verifier on a prepared image was rejected: %v", err)
	}
}

func TestPreparedImageDoesNotExemptTrustedOrOtherSidecarImages(t *testing.T) {
	for name, mutate := range map[string]func(*plan){
		"missing runtime admission": func(p *plan) { p.ImageAdmission.Admissions = p.ImageAdmission.Admissions[:1] },
		"missing agent admission":   func(p *plan) { p.ImageAdmission.Admissions = p.ImageAdmission.Admissions[1:] },
		"missing materialization":   func(p *plan) { p.TaskImageMaterializationID = nil },
		"different private image":   func(p *plan) { p.Sidecars[0].ImageRef = "registry/other@sha256:" + strings.Repeat("c", 64) },
		"same image public sidecar": func(p *plan) { p.Sidecars[0].PrivateSandbox = false; p.Sidecars[0].RoleName = "database" },
		"prepared image as controller": func(p *plan) {
			p.AgentImageRef = &p.TaskImageRef
			p.ImageAdmission.Admissions = p.ImageAdmission.Admissions[1:]
		},
	} {
		t.Run(name, func(t *testing.T) {
			p := preparedTaskPlan()
			mutate(&p)
			if err := p.validate(); err == nil || !strings.Contains(err.Error(), "coverage") {
				t.Fatalf("missing publication admission accepted: %v", err)
			}
		})
	}
	t.Run("different private image with its own admission", func(t *testing.T) {
		p := preparedTaskPlan()
		p.Sidecars[0].ImageRef = "registry/other@sha256:" + strings.Repeat("c", 64)
		admission := p.ImageAdmission.Admissions[0]
		admission.Statement.ImageRef = p.Sidecars[0].ImageRef
		p.ImageAdmission.Admissions = append(p.ImageAdmission.Admissions, admission)
		if err := p.validate(); err != nil {
			t.Fatalf("fully admitted other sandbox rejected: %v", err)
		}
	})
}

func fixturePlanPayload(t *testing.T) map[string]any {
	t.Helper()
	p := preparedTaskPlan()
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	fixture := map[string]any{
		"role_name": "fixture-server", "image_ref": "registry/fixture@sha256:" + strings.Repeat("c", 64),
		"task_fixture": true, "task_image_component": "sidecar:server", "hostname": "fixture.example",
		"argv": []string{"python3", "/server.py"}, "environment": map[string]string{},
		"resources":  map[string]int{"cpu_millis": 100, "memory_mib": 128, "ephemeral_storage_mib": 128},
		"depends_on": []string{}, "private_sandbox": false,
		"startup_probe": map[string]any{"kind": "exec", "argv": []string{"/bin/true"}, "timeout_seconds": 5,
			"period_seconds": 2, "failure_threshold": 15, "initial_delay_seconds": 5},
		"readiness_probe": map[string]any{"kind": "exec", "argv": []string{"/bin/true"}, "timeout_seconds": 5,
			"period_seconds": 2, "failure_threshold": 15},
	}
	payload["sidecars"] = append([]any{fixture}, payload["sidecars"].([]any)...)
	return payload
}

func TestDecodePlanSupportsPreparedFixtureWithoutPlatformAdmission(t *testing.T) {
	payload := fixturePlanPayload(t)
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodePlan(raw)
	if err != nil {
		t.Fatal(err)
	}
	roundtrip, err := json.Marshal(decoded)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(roundtrip, &got); err != nil {
		t.Fatal(err)
	}
	fixture := got["sidecars"].([]any)[0].(map[string]any)
	if fixture["task_fixture"] != true || fixture["task_image_component"] != "sidecar:server" || fixture["hostname"] != "fixture.example" {
		t.Fatalf("fixture metadata lost: %v", fixture)
	}
	if fixture["startup_probe"].(map[string]any)["initial_delay_seconds"] != float64(5) {
		t.Fatal("fixture startup grace lost")
	}
}

func TestFixtureRejectsForgedRoleOrControllerShape(t *testing.T) {
	for name, mutate := range map[string]func(map[string]any, map[string]any){
		"missing grant":      func(p, f map[string]any) { delete(p, "task_image_materialization_id") },
		"private sandbox":    func(p, f map[string]any) { f["private_sandbox"] = true },
		"missing component":  func(p, f map[string]any) { delete(f, "task_image_component") },
		"wrong component":    func(p, f map[string]any) { f["task_image_component"] = "sidecar:other" },
		"missing opt in":     func(p, f map[string]any) { delete(f, "task_fixture") },
		"reserved host":      func(p, f map[string]any) { f["hostname"] = "localhost" },
		"numeric host":       func(p, f map[string]any) { f["hostname"] = "127.0.0.1" },
		"hexadecimal host":   func(p, f map[string]any) { f["hostname"] = "0x08080808" },
		"dotted hex host":    func(p, f map[string]any) { f["hostname"] = "0x7f.1" },
		"mixed numeric host": func(p, f map[string]any) { f["hostname"] = "0177.0x0.0.01" },
		"host injection":     func(p, f map[string]any) { f["hostname"] = "fixture\n127.0.0.1 controller" },
		"environment":        func(p, f map[string]any) { f["environment"] = map[string]string{"HOME": "/workspace"} },
		"dependency":         func(p, f map[string]any) { f["depends_on"] = []string{"task-sandbox"} },
		"missing sandbox":    func(p, f map[string]any) { p["sidecars"] = p["sidecars"].([]any)[:2] },
		"not first":          func(p, f map[string]any) { s := p["sidecars"].([]any); s[0], s[1] = s[1], s[0] },
		"negative grace":     func(p, f map[string]any) { f["startup_probe"].(map[string]any)["initial_delay_seconds"] = -1 },
		"unbounded grace":    func(p, f map[string]any) { f["startup_probe"].(map[string]any)["initial_delay_seconds"] = 301 },
	} {
		t.Run(name, func(t *testing.T) {
			p := fixturePlanPayload(t)
			mutate(p, p["sidecars"].([]any)[0].(map[string]any))
			raw, err := json.Marshal(p)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodePlan(raw); err == nil {
				t.Fatal("forged fixture accepted")
			}
		})
	}
}

func TestLegacySidecarsOmitFixtureAndZeroProbeGrace(t *testing.T) {
	payload, err := json.Marshal(preparedTaskPlan())
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"task_fixture", "task_image_component", "hostname", "initial_delay_seconds"} {
		if strings.Contains(string(payload), `"`+field+`"`) {
			t.Fatalf("legacy payload gained %s", field)
		}
	}
}
