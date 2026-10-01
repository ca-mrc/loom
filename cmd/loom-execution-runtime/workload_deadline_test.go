package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestWorkloadDeadlineRejectsExpiredOrMalformedStartup(t *testing.T) {
	for _, raw := range []string{"not-a-deadline", "2026-09-30T01:00:00", time.Now().Add(-time.Second).Format(time.RFC3339Nano)} {
		ctx, cancel, err := workloadContext(context.Background(), raw)
		if err == nil || ctx != nil || cancel != nil {
			t.Fatalf("unsafe deadline accepted: %q, %v", raw, err)
		}
	}
}

func TestWorkloadDeadlineRetainsOriginalCutoffAndEarlierParent(t *testing.T) {
	cutoff := time.Now().Add(time.Minute).UTC()
	ctx, cancel, err := workloadContext(context.Background(), cutoff.Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	if got, ok := ctx.Deadline(); !ok || !got.Equal(cutoff) {
		t.Fatalf("deadline was refreshed: %v, %v", got, ok)
	}
	parent, stop := context.WithDeadline(context.Background(), cutoff.Add(-time.Second))
	defer stop()
	child, finish, err := workloadContext(parent, cutoff.Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	if got, _ := child.Deadline(); !got.Equal(cutoff.Add(-time.Second)) {
		t.Fatalf("extended parent cutoff: %v", got)
	}
}

func TestWorkloadDeadlineStopsRealPhaseWithoutVerifierAndRetainsOutput(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	p := timeoutHandoffPlan(workspace, "124")
	p.Main.TimeoutSeconds = 30
	p.Main.Argv = []string{"/bin/sh", "-c", "printf partial > artifact; trap 'exit 124' TERM; while :; do sleep 1; done"}
	ctx, cancel, err := workloadContext(context.Background(), time.Now().Add(300*time.Millisecond).Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	result, err := runPlan(ctx, p, workspace, output, nil)
	if err == nil || result.Status != "timed_out" || !result.PartialEvidence || len(result.Phases) != 1 {
		t.Fatalf("whole-lease deadline lost: %#v, %v", result, err)
	}
	if content, err := os.ReadFile(filepath.Join(workspace, "artifact")); err != nil || string(content) != "partial" {
		t.Fatalf("lost partial output: %q, %v", content, err)
	}
	if _, err := os.Stat(filepath.Join(workspace, "verified")); !os.IsNotExist(err) {
		t.Fatalf("verifier ran past whole-lease cutoff: %v", err)
	}
}

func TestExpiredParentDeadlineCannotStartAnyPhase(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	p := testPlan(workspace, phase{Role: "agent", Argv: []string{"/bin/sh", "-c", "touch ran"}, WorkingDirectory: workspace, TimeoutSeconds: 30})
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	result, err := runPlan(ctx, p, workspace, output, nil)
	if !errors.Is(err, context.DeadlineExceeded) || result.Status != "timed_out" || len(result.Phases) != 0 {
		t.Fatalf("expired startup is not timeout: %#v, %v", result, err)
	}
	if _, err := os.Stat(filepath.Join(workspace, "ran")); !os.IsNotExist(err) {
		t.Fatalf("expired workload launched command: %v", err)
	}
}

func TestLegacyWorkloadWithoutAbsoluteFlagPreservesParent(t *testing.T) {
	parent, stop := context.WithCancel(context.Background())
	ctx, cancel, err := workloadContext(parent, "")
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	if _, ok := ctx.Deadline(); ok {
		t.Fatal("invented deadline for legacy invocation")
	}
	stop()
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("lost parent cancellation")
	}
}

func TestExpiredDeadlineMainRejectsBeforeBrokerAndInputAccess(t *testing.T) {
	if path := os.Getenv("LOOM_TEST_DEADLINE_PLAN"); path != "" {
		flag.CommandLine = flag.NewFlagSet("runtime-deadline-test", flag.ExitOnError)
		os.Args = []string{"loom-execution-runtime", "--deadline-at", time.Now().Add(-time.Second).Format(time.RFC3339Nano), "--plan", path}
		main()
		return
	}
	p := testPlan("/workspace", phase{Role: "agent", Argv: []string{"/bin/true"}, WorkingDirectory: "/workspace", TimeoutSeconds: 30})
	body, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "plan.json")
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestExpiredDeadlineMainRejectsBeforeBrokerAndInputAccess$")
	command.Env = append(os.Environ(), "LOOM_TEST_DEADLINE_PLAN="+path, "LOOM_EXECUTION_BROKER_URL=deliberately-invalid")
	output, err := command.CombinedOutput()
	if err == nil || !strings.Contains(string(output), "workload deadline elapsed") || strings.Contains(string(output), "initialize workload broker") {
		t.Fatalf("expired main reached broker or ignored deadline: %s, %v", output, err)
	}
}
