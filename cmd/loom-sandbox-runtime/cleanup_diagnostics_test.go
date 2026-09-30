package main

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCleanupFailurePublishesOnlyFixedReason(t *testing.T) {
	cases := []struct {
		err  error
		code string
	}{
		{errCleanupPIDNamespace, "pid_namespace_invalid"},
		{errCleanupProcessOwner, "process_owner_mismatch"},
		{errCleanupProcRead, "process_inspection_failed"},
		{errCleanupSignalDenied, "cleanup_signal_denied"},
		{errCleanupSignalFailed, "cleanup_signal_failed"},
		{fmt.Errorf("private detail: %w", context.DeadlineExceeded), "cleanup_timeout"},
		{context.Canceled, "cleanup_cancelled"},
		{errors.New("private command/env/URL fixture"), "cleanup_failed"},
	}
	for _, tc := range cases {
		response := httptest.NewRecorder()
		writeCleanupFailure(response, tc.err)
		if response.Code != 409 || response.Header().Get("X-Loom-Sandbox-Error") != tc.code || response.Body.String() != "sandbox process cleanup failed\n" {
			t.Fatalf("unexpected cleanup response for %s: %v", tc.code, response)
		}
	}
}

func TestCleanupOwnershipDiagnosticContainsOnlyKernelIdentity(t *testing.T) {
	response := httptest.NewRecorder()
	writeCleanupFailure(response, fmt.Errorf("private detail: %w", &processOwnerError{
		PID: 31, ParentPID: 1, State: "S", ExpectedUID: 65532, ObservedUID: 65533,
	}))
	if response.Header().Get("X-Loom-Sandbox-Process") != "pid=31;ppid=1;state=S;uid=65533;expected_uid=65532" {
		t.Fatalf("missing bounded process identity: %v", response.Header())
	}
	if response.Body.String() != "sandbox process cleanup failed\n" {
		t.Fatal("unsafe cleanup detail in body")
	}
}

func TestCleanupTimeoutPublishesBoundedProcessSnapshot(t *testing.T) {
	response := httptest.NewRecorder()
	writeCleanupFailure(response, &cleanupDiagnosticError{
		cause: context.DeadlineExceeded,
		snapshots: []processSnapshot{
			{PID: 31, ParentPID: 1, State: "D", UID: 0, WaitChannel: "wait_on_bit", KillErrno: 0, SignalPending: true},
			{PID: 44, ParentPID: 31, State: "S", UID: 65532, WaitChannel: "0", KillErrno: int(1)},
			{PID: 90, ParentPID: 1, State: "R", UID: 1, WaitChannel: "ok", KillErrno: 0},
			{PID: 91, ParentPID: 1, State: "S", UID: 1, WaitChannel: "ok", KillErrno: 0},
			{PID: 200, ParentPID: 1, State: "S", UID: 1, WaitChannel: "../private command", KillErrno: 0},
		},
	})
	if response.Header().Get("X-Loom-Sandbox-Error") != "cleanup_timeout" {
		t.Fatalf("timeout code lost: %v", response.Header())
	}
	want := "pid=31;ppid=1;state=D;uid=0;wchan=wait_on_bit;kill_errno=0;sigkill_pending=1," +
		"pid=44;ppid=31;state=S;uid=65532;wchan=0;kill_errno=1;sigkill_pending=0," +
		"pid=90;ppid=1;state=R;uid=1;wchan=ok;kill_errno=0;sigkill_pending=0," +
		"pid=91;ppid=1;state=S;uid=1;wchan=ok;kill_errno=0;sigkill_pending=0"
	if response.Header().Get("X-Loom-Sandbox-Process") != want {
		t.Fatalf("snapshot header: %q", response.Header().Get("X-Loom-Sandbox-Process"))
	}
	if response.Body.String() != "sandbox process cleanup failed\n" || strings.Contains(response.Body.String()+response.Header().Get("X-Loom-Sandbox-Process"), "private") {
		t.Fatal("unsafe cleanup detail published")
	}
}

func TestSignalDenialPublishesErrnoInsteadOfTimeout(t *testing.T) {
	response := httptest.NewRecorder()
	writeCleanupFailure(response, &cleanupDiagnosticError{
		cause:     errCleanupSignalDenied,
		snapshots: []processSnapshot{{PID: 18, ParentPID: 1, State: "S", UID: 1000, WaitChannel: "do_wait", KillErrno: 1}},
	})
	if response.Header().Get("X-Loom-Sandbox-Error") != "cleanup_signal_denied" {
		t.Fatalf("signal denial became %s", response.Header().Get("X-Loom-Sandbox-Error"))
	}
	if response.Header().Get("X-Loom-Sandbox-Process") != "pid=18;ppid=1;state=S;uid=1000;wchan=do_wait;kill_errno=1;sigkill_pending=0" {
		t.Fatalf("missing signal snapshot: %q", response.Header().Get("X-Loom-Sandbox-Process"))
	}
}
