package main

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func timeoutHandoffPlan(workspace, exitCode string) plan {
	agent := phase{Role: "agent", Argv: []string{"/bin/sh", "-c", "trap 'exit " + exitCode + "' TERM; while :; do sleep 1; done"}, WorkingDirectory: workspace, TimeoutSeconds: 1}
	p := testPlan(workspace, agent)
	p.VerifierAfterAgentTimeout = true
	p.VerifierExecution = "in_attempt"
	p.Verifier = &phase{Role: "verifier", Argv: []string{"/bin/sh", "-c", "printf evaluated > verified"}, WorkingDirectory: workspace, TimeoutSeconds: 3}
	return p
}

func TestTimeoutHandoffRunsVerifierAndPreservesTimeout(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	p := timeoutHandoffPlan(workspace, "124")
	result, err := runPlan(context.Background(), p, workspace, output, nil)
	if err == nil || result.Status != "timed_out" || !result.PartialEvidence || len(result.Phases) != 2 {
		t.Fatalf("timeout handoff lost failure or verifier: %#v, %v", result, err)
	}
	if !result.Phases[0].TimedOut || result.Phases[0].ExitCode != 124 || result.Phases[1].ExitCode != 0 {
		t.Fatalf("incorrect phase evidence: %#v", result.Phases)
	}
	if _, err := os.Stat(filepath.Join(workspace, "verified")); err != nil {
		t.Fatal(err)
	}
}

func TestTimeoutHandoffRejectsUnsafeOrUnrequestedContinuation(t *testing.T) {
	for _, kind := range []string{"cleanup_failed", "not_opted_in", "premature_code", "cancelled"} {
		t.Run(kind, func(t *testing.T) {
			workspace, output := t.TempDir(), t.TempDir()
			p := timeoutHandoffPlan(workspace, "124")
			ctx := context.Background()
			switch kind {
			case "cleanup_failed":
				p = timeoutHandoffPlan(workspace, "1")
			case "not_opted_in":
				p.VerifierAfterAgentTimeout = false
			case "premature_code":
				p.Main.Argv = []string{"/bin/sh", "-c", "exit 124"}
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
				time.AfterFunc(100*time.Millisecond, cancel)
			}
			result, err := runPlan(ctx, p, workspace, output, nil)
			if err == nil || len(result.Phases) != 1 {
				t.Fatalf("unsafe continuation: %#v, %v", result, err)
			}
			if _, err := os.Stat(filepath.Join(workspace, "verified")); !os.IsNotExist(err) {
				t.Fatalf("verifier ran: %v", err)
			}
		})
	}
}

func TestTimeoutHandoffRetainsVerifierFailure(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	p := timeoutHandoffPlan(workspace, "124")
	p.Verifier.Argv = []string{"/bin/sh", "-c", "exit 7"}
	result, err := runPlan(context.Background(), p, workspace, output, nil)
	if err == nil || result.Status != "timed_out" || len(result.Phases) != 2 || result.Phases[1].ExitCode != 7 {
		t.Fatalf("lost agent timeout or verifier failure: %#v, %v", result, err)
	}
}

func TestRuntimeInjectsAuthoritativePhaseDeadline(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	agent := phase{Role: "agent", Argv: []string{"/bin/sh", "-c", "printf '%s,%s' \"$LOOM_EXECUTION_PHASE_DEADLINE\" \"$LOOM_EXECUTION_TERMINATION_GRACE_SECONDS\" > deadline"}, WorkingDirectory: workspace, TimeoutSeconds: 3,
		Environment: map[string]string{"LOOM_EXECUTION_PHASE_DEADLINE": "9999999999", "LOOM_EXECUTION_TERMINATION_GRACE_SECONDS": "999"}}
	start := time.Now()
	_, err := runPlan(context.Background(), testPlan(workspace, agent), workspace, output, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(workspace, "deadline"))
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(string(b), ",")
	seconds, err := strconv.ParseFloat(parts[0], 64)
	if err != nil || parts[1] != "1" {
		t.Fatalf("missing owned bounds: %q (%v)", b, err)
	}
	deadline := time.Unix(0, int64(seconds*1e9))
	if deadline.Before(start.Add(2900*time.Millisecond)) || deadline.After(time.Now().Add(3*time.Second)) {
		t.Fatalf("deadline not bound to parent phase: %v", deadline)
	}
}

func TestCollectionFinalizationBudgetPreservesAgentDeadline(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	p := timeoutHandoffPlan(workspace, "124")
	p.Main.FinalizationTimeoutSeconds = 2
	p.Main.Argv = []string{"/bin/sh", "-c", "trap 'sleep 1.2; printf captured > collected; exit 124' TERM; while :; do sleep 1; done"}
	start := time.Now()
	var agentDeadline time.Time
	result, err := runPlan(context.Background(), p, workspace, output, nil, func(role string, cutoff time.Time) {
		if role == "agent" && !cutoff.IsZero() {
			agentDeadline = cutoff
		}
	})
	if err == nil || len(result.Phases) != 2 || result.Phases[0].ExitCode != 124 {
		t.Fatalf("collection budget lost safe handoff: %#v, %v", result, err)
	}
	if agentDeadline.After(start.Add(1500 * time.Millisecond)) {
		t.Fatal("collection extended model deadline")
	}
	if _, err := os.Stat(filepath.Join(workspace, "collected")); err != nil {
		t.Fatal(err)
	}
}
