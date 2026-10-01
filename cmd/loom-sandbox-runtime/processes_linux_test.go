//go:build linux

package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

func TestReapedProcessDirectoryIsNotForeignOwner(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("native sandboxes run as nonroot")
	}
	child := exec.Command("/bin/cat")
	input, err := child.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		input.Close()
		if child.ProcessState == nil {
			child.Process.Kill()
			child.Wait()
		}
	})
	directory, err := os.Open(filepath.Join("/proc", strconv.Itoa(child.Process.Pid)))
	if err != nil {
		t.Fatal(err)
	}
	defer directory.Close()
	input.Close()
	if err = child.Wait(); err != nil {
		t.Fatal(err)
	}
	// pid_getattr returns a root-owned inode for a directory held past reap.
	// A path stat can observe the same result between revalidation and getattr.
	info, err := directory.Stat()
	if err != nil {
		t.Fatal(err)
	}
	if info.Sys().(*syscall.Stat_t).Uid != 0 {
		t.Fatal("kernel did not expose reaped proc ownership")
	}
	_, err = sandboxProcessState(fmt.Sprintf("/proc/self/fd/%d", directory.Fd()), os.Geteuid())
	if !errors.Is(err, os.ErrNotExist) && !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("reaped descendant treated as a live foreign process: %v", err)
	}
}

func TestProcessStatusUsesEffectiveUIDAndRejectsInvalidInspection(t *testing.T) {
	cases := []struct {
		name, status, state string
		want                error
	}{
		{"live", "State:\tS (sleeping)\nUid:\t0\t65532\t0\t0\n", "S", nil},
		{"zombie", "State:\tZ (zombie)\nUid:\t65532\t65532\t65532\t65532\n", "Z", nil},
		{"foreign_effective_uid", "State:\tS (sleeping)\nUid:\t65532\t65533\t65532\t65532\n", "", errCleanupProcessOwner},
		{"external_probe_root", "State:\tS (sleeping)\nPPid:\t0\nUid:\t0\t0\t0\t0\n", "", nil},
		{"external_probe_after_setuid", "State:\tR (running)\nPPid:\t0\nUid:\t65532\t65532\t65532\t65532\n", "", nil},
		{"adopted_foreign_child", "State:\tZ (zombie)\nPPid:\t1\nUid:\t0\t0\t0\t0\n", "", errCleanupProcessOwner},
		{"missing_parent", "State:\tS\nUid:\t65532\t65532\t65532\t65532\n", "", errCleanupProcRead},
		{"invalid_parent", "State:\tS\nPPid:\t-1\nUid:\t65532\t65532\t65532\t65532\n", "", errCleanupProcRead},
		{"missing_uid", "State:\tS (sleeping)\n", "", errCleanupProcRead},
		{"missing_state", "Uid:\t65532\t65532\t65532\t65532\n", "", errCleanupProcRead},
		{"truncated_uid", "State:\tS\nUid:\t65532\t65532\n", "", errCleanupProcRead},
		{"invalid_uid", "State:\tS\nUid:\t65532\tbad\t65532\t65532\n", "", errCleanupProcRead},
		{"bad_signal_mask", "State:\tS (sleeping)\nUid:\t65532\t65532\t65532\t65532\nSigPnd:\tzz\n", "", errCleanupProcRead},
		{"negative_uid", "State:\tS\nUid:\t65532\t-1\t65532\t65532\n", "", errCleanupProcRead},
		{"empty", "", "", errCleanupProcRead},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			directory := t.TempDir()
			if !strings.Contains(tc.status, "PPid:") && tc.name != "missing_parent" {
				tc.status += "PPid:\t1\n"
			}
			if err := os.WriteFile(filepath.Join(directory, "status"), []byte(tc.status), 0600); err != nil {
				t.Fatal(err)
			}
			state, err := sandboxProcessState(directory, 65532)
			if state != tc.state || !errors.Is(err, tc.want) {
				t.Fatalf("got state=%q error=%v; want state=%q error=%v", state, err, tc.state, tc.want)
			}
		})
	}
	// A real read failure must not be treated as an exited process.
	directory := t.TempDir()
	if err := os.Mkdir(filepath.Join(directory, "status"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := sandboxProcessState(directory, 65532); !errors.Is(err, errCleanupProcRead) {
		t.Fatalf("ignored read failure: %v", err)
	}
}

func TestSignalClassificationSeparatesExitFromRefusal(t *testing.T) {
	if errno, kind := classifySignal(nil); errno != 0 || kind != signalSent {
		t.Fatalf("successful kill: errno=%d kind=%d", errno, kind)
	}
	if _, kind := classifySignal(syscall.ESRCH); kind != signalGone {
		t.Fatalf("exited process was not ESRCH: %d", kind)
	}
	if errno, kind := classifySignal(syscall.EPERM); kind != signalDenied || errno != int(syscall.EPERM) {
		t.Fatalf("refused kill: errno=%d kind=%d", errno, kind)
	}
	if _, kind := classifySignal(syscall.EINVAL); kind != signalFailed {
		t.Fatalf("other signal error was not a signal failure: %d", kind)
	}
}

func TestCleanupSnapshotReadsPendingSignalAndWaitChannel(t *testing.T) {
	directory := t.TempDir()
	status := "State:\tD (disk sleep)\nPPid:\t1\nUid:\t0\t0\t0\t0\nShdPnd:\t0000000000000100\n"
	if err := os.WriteFile(filepath.Join(directory, "status"), []byte(status), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "wchan"), []byte("wait_on_bit\n"), 0600); err != nil {
		t.Fatal(err)
	}
	snapshot, err := inspectSandboxProcess(directory, 0)
	if err != nil || snapshot.State != "D" || !snapshot.SignalPending || snapshot.UID != 0 || snapshot.ParentPID != 1 {
		t.Fatalf("snapshot identity: %#v %v", snapshot, err)
	}
	snapshot.PID = 31
	snapshot.WaitChannel = boundedWaitChannel(directory)
	header, ok := snapshot.diagnostic()
	if !ok || header != "pid=31;ppid=1;state=D;uid=0;wchan=wait_on_bit;kill_errno=0;sigkill_pending=1" {
		t.Fatalf("published snapshot: %q %v", header, ok)
	}
	if err := os.WriteFile(filepath.Join(directory, "wchan"), []byte("../secret command"), 0600); err != nil {
		t.Fatal(err)
	}
	if boundedWaitChannel(directory) != "unavailable" {
		t.Fatal("unsafe wait channel published")
	}
}

func TestRootSandboxCanCleanUpTaskDescendantsThatDropUID(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "status"), []byte("State:\tS\nPPid:\t1\nUid:\t1001\t1001\t1001\t1001\n"), 0600); err != nil {
		t.Fatal(err)
	}
	state, err := sandboxProcessState(directory, 0)
	if err != nil || state != "S" {
		t.Fatalf("root sandbox cannot clean its dropped-UID descendant: %v", err)
	}
}
