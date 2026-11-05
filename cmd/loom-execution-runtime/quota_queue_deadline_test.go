package main

import (
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
