package main

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestInvalidOrExpiredDeadlineDoesNotLaunchBuild(t *testing.T) {
	for _, raw := range []string{"", "invalid", "2026-09-30T01:00:00", time.Now().Add(-time.Second).Format(time.RFC3339Nano)} {
		marker := filepath.Join(t.TempDir(), "ran")
		if got := supervise(context.Background(), raw, []string{"/bin/sh", "-c", "touch \"$1\"", "build", marker}, time.Millisecond); got != 124 {
			t.Fatalf("deadline %q returned %d", raw, got)
		}
		if _, err := os.Stat(marker); !os.IsNotExist(err) {
			t.Fatalf("expired build executed: %v", err)
		}
	}
}

func TestBuildSupervisorPreservesCommandExit(t *testing.T) {
	for _, want := range []int{0, 17, 124} {
		argv := []string{"/bin/sh", "-c", "exit \"$1\"", "build", strconv.Itoa(want)}
		if got := supervise(context.Background(), time.Now().Add(time.Minute).Format(time.RFC3339Nano), argv, time.Millisecond); got != want {
			t.Fatalf("exit %d became %d", want, got)
		}
	}
}

func TestOriginalCutoffStopsBuildAndAllowsBoundedTermination(t *testing.T) {
	marker := filepath.Join(t.TempDir(), "terminated")
	cutoff := time.Now().Add(300 * time.Millisecond)
	argv := []string{"/bin/sh", "-c", "trap 'printf stopped > \"$1\"; exit 0' TERM; while :; do sleep 1; done", "build", marker}
	got := supervise(context.Background(), cutoff.Format(time.RFC3339Nano), argv, 500*time.Millisecond)
	if got != 124 || time.Now().Before(cutoff) || time.Since(cutoff) > 2*time.Second {
		t.Fatalf("cutoff not honored: exit %d at %s", got, time.Now())
	}
	if body, err := os.ReadFile(marker); err != nil || string(body) != "stopped" {
		t.Fatalf("TERM grace lost: %q, %v", body, err)
	}
}

func TestBuildIgnoringTerminationCannotExtendGrace(t *testing.T) {
	cutoff := time.Now().Add(200 * time.Millisecond)
	argv := []string{"/bin/sh", "-c", "trap '' TERM; while :; do sleep 1; done"}
	got := supervise(context.Background(), cutoff.Format(time.RFC3339Nano), argv, 100*time.Millisecond)
	if got != 124 || time.Since(cutoff) < 100*time.Millisecond || time.Since(cutoff) > 2*time.Second {
		t.Fatalf("unbounded termination: exit %d after %s", got, time.Since(cutoff))
	}
}

func TestCancelledParentDoesNotLaunchBuild(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	marker := filepath.Join(t.TempDir(), "ran")
	got := supervise(ctx, time.Now().Add(time.Minute).Format(time.RFC3339Nano), []string{"/bin/sh", "-c", "touch \"$1\"", "build", marker}, time.Millisecond)
	if got != 143 {
		t.Fatalf("cancellation returned %d", got)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("cancelled build executed: %v", err)
	}
}
