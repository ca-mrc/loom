package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testPlan(workspace string, agent phase) plan {
	now := time.Now().UTC()
	taskImage := "registry/task@sha256:" + strings.Repeat("3", 64)
	runtimeImage := "registry/runtime@sha256:" + strings.Repeat("4", 64)
	admission := func(imageRef string) signedImageAdmission {
		return signedImageAdmission{
			Statement: imageAdmissionStatement{
				SchemaVersion: "loom.image-admission-statement.v1", ImageRef: imageRef,
				Platform: "linux/x86_64", SBOMSHA256: "sha256:" + strings.Repeat("7", 64),
				ProvenanceSHA256:             "sha256:" + strings.Repeat("8", 64),
				VulnerabilityReportSHA256:    "sha256:" + strings.Repeat("9", 64),
				PolicySHA256:                 "sha256:" + strings.Repeat("a", 64),
				HighestVulnerabilitySeverity: "medium", IssuedAt: now, ExpiresAt: now.Add(time.Hour),
			},
			SigningKeyID: "test-builder", SignatureBase64: base64.StdEncoding.EncodeToString(make([]byte, 64)),
		}
	}
	return plan{
		SchemaVersion: "loom.execution-runtime-plan.v1", CandidateSHA: strings.Repeat("1", 40),
		TaskRevisionSHA256:    "sha256:" + strings.Repeat("2", 64),
		CommandIdentitySHA256: "sha256:" + strings.Repeat("6", 64), ExecutionRole: "attempt",
		ExecutionClassID: "linux-amd64-cpu-pod-v1", Composition: "init_payload",
		TaskImageRef:        taskImage,
		RuntimeImageRef:     runtimeImage,
		RuntimeBinarySHA256: "sha256:" + strings.Repeat("5", 64),
		ImageAdmission: executionImageAdmission{
			SchemaVersion: "loom.execution-image-admission.v1",
			Admissions:    []signedImageAdmission{admission(taskImage), admission(runtimeImage)},
		},
		RunAsUser: 65532, RunAsGroup: 65532, FSGroup: 65532,
		TaskResources: resources{CPUMillis: 100, MemoryMiB: 128, EphemeralStorageMiB: 128},
		WorkspaceMiB:  128, RuntimeVolumeMiB: 32, TerminationGraceSec: 1,
		Main: agent, VerifierExecution: "skipped", Sidecars: []sidecar{},
		MaxLogBytesPerStream: 32, MaxArtifactBytes: 1024,
	}
}

func TestRunPlanCapturesBoundedEvidenceAndAtomicResult(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	agent := phase{
		Role: "agent", Argv: []string{"/bin/sh", "-c", "printf 'abcdefghijklmnopqrstuvwxyz0123456789'; printf 'error' >&2"},
		WorkingDirectory: workspace, TimeoutSeconds: 5, Environment: map[string]string{"LOOM_PHASE": "agent"},
	}
	result, err := runPlan(context.Background(), testPlan(workspace, agent), workspace, output, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != "succeeded" || len(result.Phases) != 1 || !result.Phases[0].Stdout.Truncated {
		t.Fatalf("unexpected result: %#v", result)
	}
	if result.Phases[0].Stdout.BytesSeen != 36 || result.Phases[0].Stdout.BytesSaved != 32 {
		t.Fatalf("unexpected stdout evidence: %#v", result.Phases[0].Stdout)
	}
	path := filepath.Join(output, "result.json")
	if err := writeResult(path, result); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("temporary result remains: %v", err)
	}
	summaryPath := filepath.Join(output, "termination-message")
	if err := writeTerminationSummary(summaryPath, result, &outputCommitEvidence{
		UploadSessionID:       "0194d739-8bec-7b7b-88f5-62f7cbd42cb3",
		ManifestSHA256:        "sha256:" + strings.Repeat("1", 64),
		CommittedMarkerSHA256: "sha256:" + strings.Repeat("2", 64),
	}); err != nil {
		t.Fatal(err)
	}
	var summary terminationSummary
	payload, err := os.ReadFile(summaryPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(payload, &summary); err != nil {
		t.Fatal(err)
	}
	if summary.Status != "succeeded" || summary.ResultPath != "result.json" {
		t.Fatalf("unexpected termination summary: %#v", summary)
	}
}

func TestSandboxCleanupTimeoutIsRuntimeError(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	script := "mkdir -p .loom/agent && printf '%s' '{\"exception_type\":\"SandboxRPCError\",\"exception_message\":\"sandbox stop_processes failed (HTTP 409; cleanup_timeout)\"}' > .loom/agent/exception.json && exit 1"
	agent := phase{
		Role: "agent", Argv: []string{"/bin/sh", "-c", script},
		WorkingDirectory: workspace, TimeoutSeconds: 5,
	}
	result, err := runPlan(context.Background(), testPlan(workspace, agent), workspace, output, nil)
	if err == nil || result.Status != "runtime_error" || !result.PartialEvidence {
		t.Fatalf("cleanup timeout was not a runtime error: result=%#v err=%v", result, err)
	}
}

func TestSandboxCleanupSnapshotIsRuntimeError(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	message := "sandbox stop_processes failed (HTTP 409; cleanup_timeout; pid=31;ppid=1;state=D;uid=0;wchan=wait_on_bit;kill_errno=0;sigkill_pending=1)"
	if err := os.MkdirAll(filepath.Join(workspace, ".loom", "agent"), 0755); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]string{
		"exception_type": "SandboxRPCError", "exception_message": message,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, ".loom", "agent", "exception.json"), payload, 0600); err != nil {
		t.Fatal(err)
	}
	agent := phase{
		Role: "agent", Argv: []string{"/bin/sh", "-c", "exit 1"},
		WorkingDirectory: workspace, TimeoutSeconds: 5,
	}
	result, err := runPlan(context.Background(), testPlan(workspace, agent), workspace, output, nil)
	if err == nil || result.Status != "runtime_error" {
		t.Fatalf("cleanup snapshot was not a runtime error: result=%#v err=%v", result, err)
	}
}

func TestForgedCleanupDetailRemainsTaskError(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	message := "sandbox stop_processes failed (HTTP 409; cleanup_timeout; command=private)"
	if err := os.MkdirAll(filepath.Join(workspace, ".loom", "agent"), 0755); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]string{
		"exception_type": "SandboxRPCError", "exception_message": message,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, ".loom", "agent", "exception.json"), payload, 0600); err != nil {
		t.Fatal(err)
	}
	agent := phase{
		Role: "agent", Argv: []string{"/bin/sh", "-c", "exit 1"},
		WorkingDirectory: workspace, TimeoutSeconds: 5,
	}
	result, err := runPlan(context.Background(), testPlan(workspace, agent), workspace, output, nil)
	if err == nil || result.Status != "task_error" {
		t.Fatalf("forged cleanup detail was reclassified: result=%#v err=%v", result, err)
	}
}

func TestAgentCrashRemainsTaskError(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	agent := phase{
		Role: "agent", Argv: []string{"/bin/sh", "-c", "exit 1"},
		WorkingDirectory: workspace, TimeoutSeconds: 5,
	}
	result, err := runPlan(context.Background(), testPlan(workspace, agent), workspace, output, nil)
	if err == nil || result.Status != "task_error" {
		t.Fatalf("agent crash was reclassified: result=%#v err=%v", result, err)
	}
}

func TestRunPlanTimeoutTerminatesProcessGroupAndRetainsPartialEvidence(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	agent := phase{
		Role: "agent", Argv: []string{"/bin/sh", "-c", "trap 'exit 0' TERM; while :; do sleep 1; done"},
		WorkingDirectory: workspace, TimeoutSeconds: 1,
	}
	started := time.Now()
	result, err := runPlan(context.Background(), testPlan(workspace, agent), workspace, output, nil)
	if err == nil || result.Status != "timed_out" || !result.PartialEvidence {
		t.Fatalf("timeout did not fail closed: result=%#v err=%v", result, err)
	}
	if time.Since(started) > 4*time.Second {
		t.Fatalf("termination exceeded bounded grace: %s", time.Since(started))
	}
}

func TestSeparateVerifierFailureHasDistinctDurableStatusAndRoles(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	p := testPlan(workspace, phase{
		Role: "verifier", Argv: []string{"/bin/sh", "-c", "printf verifier-failed >&2; exit 7"},
		WorkingDirectory: workspace, TimeoutSeconds: 5,
	})
	p.ExecutionRole = "verifier"
	result, err := runPlan(context.Background(), p, workspace, output, nil)
	if err == nil || result.Status != "verifier_error" || !result.PartialEvidence {
		t.Fatalf("verifier failure was not retained: result=%#v err=%v", result, err)
	}
	if strings.Join(result.ContainerRoles, ",") != "execution,verifier" {
		t.Fatalf("verifier roles are incomplete: %#v", result.ContainerRoles)
	}
	if result.Phases[0].ExitCode != 7 {
		t.Fatalf("verifier exit status was not retained: %#v", result.Phases[0])
	}
}

func TestRunPlanHidesBrokerIdentityAndOwnsGatewayEnvironment(t *testing.T) {
	t.Setenv("LOOM_EXECUTION_LEASE_ID", "should-not-reach-task")
	workspace, output := t.TempDir(), t.TempDir()
	agent := phase{
		Role: "agent",
		Argv: []string{
			"/bin/sh",
			"-c",
			`test -z "$LOOM_EXECUTION_LEASE_ID" && test "$OPENAI_BASE_URL" = "http://127.0.0.1:43123/v1"`,
		},
		WorkingDirectory: workspace,
		TimeoutSeconds:   5,
		Environment: map[string]string{
			"LOOM_EXECUTION_LEASE_ID": "plan-injection",
			"OPENAI_BASE_URL":         "http://attacker.invalid",
		},
	}
	result, err := runPlan(
		context.Background(),
		testPlan(workspace, agent),
		workspace,
		output,
		map[string]string{"OPENAI_BASE_URL": "http://127.0.0.1:43123/v1"},
	)
	if err != nil || result.Status != "succeeded" {
		t.Fatalf("runtime-owned environment was not enforced: result=%#v err=%v", result, err)
	}
}

func TestLoadPlanRejectsUnknownAndMutableFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plan.json")
	payload := `{"schema_version":"loom.execution-runtime-plan.v1","unknown":true}`
	if err := os.WriteFile(path, []byte(payload), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadPlan(path); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown field accepted: %v", err)
	}
}

func TestPlanValidationMatchesBoundedWorkspaceAndSidecarContract(t *testing.T) {
	p := testPlan("/workspaceevil", phase{
		Role: "agent", Argv: []string{"/bin/true"}, WorkingDirectory: "/workspaceevil", TimeoutSeconds: 1,
	})
	if err := p.validate(); err == nil {
		t.Fatal("workspace prefix escape was accepted")
	}
	p = testPlan("/workspace", phase{
		Role: "agent", Argv: []string{"/bin/true"}, WorkingDirectory: "/workspace", TimeoutSeconds: 1,
	})
	p.Sidecars = []sidecar{{
		RoleName: "verifier", ImageRef: "registry/sidecar@sha256:" + strings.Repeat("7", 64),
		Argv: []string{"/bin/true"}, Resources: resources{CPUMillis: 1, MemoryMiB: 1, EphemeralStorageMiB: 1},
		StartupProbe:   probe{Kind: "exec", TimeoutSeconds: 1, PeriodSeconds: 1, FailureThreshold: 1, Argv: []string{"/bin/true"}},
		ReadinessProbe: probe{Kind: "exec", TimeoutSeconds: 1, PeriodSeconds: 1, FailureThreshold: 1, Argv: []string{"/bin/true"}},
	}}
	if err := p.validate(); err == nil {
		t.Fatal("reserved sidecar role was accepted")
	}
}

func TestPhaseWorkingDirectoryBoundary(t *testing.T) {
	for _, directory := range []string{"/app", "/workspace", "/workspace/task-dir"} {
		item := phase{Role: "agent", Argv: []string{"/bin/pwd"}, WorkingDirectory: directory, TimeoutSeconds: 1}
		if err := item.validate(); err != nil {
			t.Fatalf("valid cwd %q rejected: %v", directory, err)
		}
	}
	for _, directory := range []string{"/app/subdir", "/app-other", "/app/.", "/app/../app", "/workspace/../app", "/workspaceevil", "/etc"} {
		item := phase{Role: "agent", Argv: []string{"/bin/pwd"}, WorkingDirectory: directory, TimeoutSeconds: 1}
		if err := item.validate(); err == nil {
			t.Fatalf("invalid cwd %q accepted by contract", directory)
		}
		if _, err := runPhase(context.Background(), item, 1, "/workspace", t.TempDir(), 1024, time.Second, nil); err == nil || !strings.Contains(err.Error(), "working directory") {
			t.Fatalf("invalid cwd %q passed runtime boundary: %v", directory, err)
		}
	}
	t.Run("workspace-child", func(t *testing.T) {
		workspace, output := t.TempDir(), t.TempDir()
		directory := filepath.Join(workspace, "task-dir")
		if err := os.Mkdir(directory, 0700); err != nil {
			t.Fatal(err)
		}
		item := phase{Role: "agent", Argv: []string{"/bin/sh", "-c", "exit 0"}, WorkingDirectory: directory, TimeoutSeconds: 1}
		result, err := runPhase(context.Background(), item, 1, workspace, output, 1024, time.Second, nil)
		if err != nil || result.ExitCode != 0 {
			t.Fatalf("workspace child cwd rejected: %v", err)
		}
	})
	t.Run("trusted-app", func(t *testing.T) {
		if info, err := os.Stat("/app"); err != nil || !info.IsDir() {
			t.Skip("run in Linux test container with /app to execute trusted cwd")
		}
		output := t.TempDir()
		item := phase{Role: "agent", Argv: []string{"/bin/pwd"}, WorkingDirectory: "/app", TimeoutSeconds: 1}
		result, err := runPhase(context.Background(), item, 1, t.TempDir(), output, 1024, time.Second, nil)
		if err != nil || result.ExitCode != 0 {
			t.Fatalf("trusted cwd rejected: %v", err)
		}
		body, err := os.ReadFile(filepath.Join(output, "01-agent.stdout"))
		if err != nil || strings.TrimSpace(string(body)) != "/app" {
			t.Fatalf("controller did not run in /app: %q %v", body, err)
		}
	})
}

func TestPlanValidationRejectsImageAdmissionDriftButNotMetadataExpiry(t *testing.T) {
	base := func() plan {
		return testPlan("/workspace", phase{
			Role: "agent", Argv: []string{"/bin/true"}, WorkingDirectory: "/workspace", TimeoutSeconds: 1,
		})
	}
	p := base()
	p.ImageAdmission.Admissions = p.ImageAdmission.Admissions[:1]
	if err := p.validate(); err == nil || !strings.Contains(err.Error(), "coverage") {
		t.Fatalf("missing image admission accepted: %v", err)
	}
	p = base()
	p.ImageAdmission.Admissions[0].Statement.HighestVulnerabilitySeverity = "high"
	if err := p.validate(); err != nil {
		t.Fatalf("nonblocking high vulnerability evidence was rejected: %v", err)
	}
	p = base()
	p.ImageAdmission.Admissions[0].Statement.HighestVulnerabilitySeverity = "unknown"
	if err := p.validate(); err != nil {
		t.Fatalf("nonblocking unknown vulnerability evidence was rejected: %v", err)
	}
	p = base()
	p.ImageAdmission.Admissions[0].Statement.HighestVulnerabilitySeverity = "critical"
	if err := p.validate(); err == nil || !strings.Contains(err.Error(), "vulnerability") {
		t.Fatalf("critical vulnerability admission accepted: %v", err)
	}
	p = base()
	p.ImageAdmission.Admissions[0].Statement.HighestVulnerabilitySeverity = "invalid"
	if err := p.validate(); err == nil || !strings.Contains(err.Error(), "severity") {
		t.Fatalf("invalid vulnerability severity accepted: %v", err)
	}
	p = base()
	p.ImageAdmission.Admissions[0].Statement.ExpiresAt = time.Now().Add(-time.Minute)
	p.ImageAdmission.Admissions[0].Statement.IssuedAt = time.Now().Add(-2 * time.Hour)
	if err := p.validate(); err != nil {
		t.Fatalf("metadata expiry became a runtime renewal gate: %v", err)
	}
	p = base()
	p.ImageAdmission.Admissions[0].Statement.IssuedAt = time.Now().Add(10 * time.Minute)
	p.ImageAdmission.Admissions[0].Statement.ExpiresAt = time.Now().Add(time.Hour)
	if err := p.validate(); err == nil || !strings.Contains(err.Error(), "lifetime") {
		t.Fatalf("future-issued admission accepted: %v", err)
	}
	p = base()
	p.ImageAdmission.Admissions[0].SignatureBase64 = "not-base64"
	if err := p.validate(); err == nil || !strings.Contains(err.Error(), "signature") {
		t.Fatalf("malformed image admission signature accepted: %v", err)
	}
}

func TestMaterializeCopiesOnlyDigestVerifiedRuntimeAndPlan(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.ReadFile(executable)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(binary)
	workspace := "/workspace"
	p := testPlan(workspace, phase{
		Role: "agent", Argv: []string{"/bin/true"}, WorkingDirectory: workspace, TimeoutSeconds: 1,
	})
	p.RuntimeBinarySHA256 = "sha256:" + hex.EncodeToString(digest[:])
	p.AgentImageRef = &p.TaskImageRef
	probe := probe{Kind: "exec", Argv: []string{"/loom/bin/loom-sandbox-runtime", "--check-socket", "/loom/sandboxes/task-sandbox/sandbox.sock"}, TimeoutSeconds: 2, PeriodSeconds: 2, FailureThreshold: 30}
	p.Sidecars = []sidecar{{RoleName: "task-sandbox", PrivateSandbox: true, ImageRef: p.TaskImageRef, Argv: []string{"/loom/bin/loom-sandbox-runtime", "--socket", "/loom/sandboxes/task-sandbox/sandbox.sock"}, Resources: p.TaskResources, StartupProbe: probe, ReadinessProbe: probe}}
	payload, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	destination := t.TempDir()
	runtimePath := filepath.Join(destination, "runtime")
	planPath := filepath.Join(destination, "plan.json")
	sandboxPath := filepath.Join(destination, "sandbox")
	sandboxSource := filepath.Join(destination, "sandbox-source")
	sandboxRoot := filepath.Join(destination, "sandboxes")
	if err := os.MkdirAll(filepath.Join(sandboxRoot, "task-sandbox"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sandboxSource, []byte("bundled sandbox binary"), 0555); err != nil {
		t.Fatal(err)
	}
	err = materialize([]string{
		"--encoded-plan", base64.RawURLEncoding.EncodeToString(payload),
		"--runtime-dest", runtimePath, "--plan-dest", planPath,
		"--sandbox-source", sandboxSource, "--sandbox-dest", sandboxPath,
		"--sandbox-root", sandboxRoot,
	})
	if err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(runtimePath); err != nil || info.Mode().Perm() != 0o555 {
		t.Fatalf("runtime mode mismatch: info=%v err=%v", info, err)
	}
	if info, err := os.Stat(sandboxPath); err != nil || info.Mode().Perm() != 0o555 {
		t.Fatalf("sandbox mode mismatch: info=%v err=%v", info, err)
	}
	if data, err := os.ReadFile(sandboxPath); err != nil || string(data) != "bundled sandbox binary" {
		t.Fatalf("sandbox materialization failed: %v", err)
	}
	for _, name := range []string{"hosts", "resolv.conf"} {
		want, err := os.ReadFile(filepath.Join("/etc", name))
		if err != nil {
			t.Fatal(err)
		}
		got, err := os.ReadFile(filepath.Join(sandboxRoot, "task-sandbox", "network", name))
		if err != nil || string(got) != string(want) {
			t.Fatalf("private network file %s differs: %v", name, err)
		}
	}
	if err := materialize([]string{
		"--encoded-plan", base64.RawURLEncoding.EncodeToString(payload),
		"--runtime-dest", runtimePath, "--plan-dest", planPath,
	}); err == nil {
		t.Fatal("materialize overwrote an existing runtime")
	}
	materializedPlan, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(materializedPlan) != string(payload) {
		t.Fatal("materialized plan bytes changed")
	}
}
