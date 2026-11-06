package main

import (
	"context"
	"testing"
	"time"
)

func TestExtendPhaseDeadlineKeepsThePhaseContext(t *testing.T) {
	broker := &workloadBroker{}
	updates := broker.ensurePhaseUpdates()
	broker.setPhase("agent", time.Now().Add(time.Hour))
	ctx := broker.phaseContext
	broker.extendPhaseDeadline(time.Now().Add(2 * time.Hour))
	select {
	case <-ctx.Done():
		t.Fatal("extension cancelled the phase")
	default:
	}
	if !broker.phaseDeadline.After(time.Now().Add(90 * time.Minute)) {
		t.Fatalf("deadline = %s", broker.phaseDeadline)
	}
	select {
	case got := <-updates:
		if !got.After(time.Now().Add(90 * time.Minute)) {
			t.Fatalf("update = %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("process timer was not notified")
	}
}

// A real subprocess outlives its original one-second phase deadline. The
// moved deadline must keep it running, and the evidence must not call it timed
// out. The control run with no updates still times out at the original limit.
func TestRunPhaseFollowsMovedDeadlineForSubprocess(t *testing.T) {
	workspace := t.TempDir()
	output := t.TempDir()
	updates := make(chan time.Time, 1)
	updates <- time.Now().Add(4 * time.Second)
	item := phase{
		Role: "agent", Argv: []string{"/bin/sh", "-c", "sleep 2"}, WorkingDirectory: workspace,
		TimeoutSeconds: 1, DeadlineUpdates: updates,
	}
	evidence, err := runPhase(context.Background(), item, 1, workspace, output, 1024, time.Second, nil)
	if err != nil {
		t.Fatalf("moved deadline cancelled the phase: %v", err)
	}
	if evidence.TimedOut || evidence.ExitCode != 0 {
		t.Fatalf("evidence = %+v", evidence)
	}
}

func TestRunPhaseWithoutUpdatesKeepsFixedTimeout(t *testing.T) {
	workspace := t.TempDir()
	item := phase{
		Role: "agent", Argv: []string{"/bin/sh", "-c", "sleep 3"}, WorkingDirectory: workspace,
		TimeoutSeconds: 1,
	}
	evidence, err := runPhase(context.Background(), item, 1, workspace, t.TempDir(), 1024, 100*time.Millisecond, nil)
	if err == nil || !evidence.TimedOut {
		t.Fatalf("fixed timeout not enforced: err=%v evidence=%+v", err, evidence)
	}
}
