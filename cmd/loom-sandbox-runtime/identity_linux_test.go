//go:build linux

package main

import (
	"os"
	"os/exec"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestFrozenIdentityOwnsCommandAndFile(t *testing.T) {
	id := &guestTaskIdentity{uid: 65532, gid: 65532, groups: []uint32{1004}, home: "/home/agent"}
	cmd := exec.Command("/bin/true")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	applyTaskCredential(cmd, id)
	cred := cmd.SysProcAttr.Credential
	if cred == nil || cred.Uid != 65532 || cred.Gid != 65532 || !slices.Equal(cred.Groups, []uint32{1004}) {
		t.Fatalf("%+v", cred)
	}
	if os.Geteuid() != 0 {
		t.Skip("observing another uid requires root")
	}
	// t.TempDir's root-owned 0700 parent is not traversable after setfsuid.
	// Keep this fixture directly below the shared temp root so the frozen UID
	// can reach the writable directory whose ownership behavior is under test.
	dir, err := os.MkdirTemp("", "loom-frozen-identity-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Errorf("remove owned identity fixture: %v", err)
		}
	})
	if err := os.Chmod(dir, 0777); err != nil {
		t.Fatal(err)
	}
	path := dir + "/owned"
	if err := withTaskFS(id, func() error {
		return writeFile(path, strings.NewReader("task"), 0644)
	}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("file stat unavailable")
	}
	if stat.Uid != 65532 || stat.Gid != 65532 {
		t.Fatalf("file owner uid=%d gid=%d", stat.Uid, stat.Gid)
	}
	observed := exec.Command("/usr/bin/id", "-u")
	observed.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	server := runtimeServer{identity: id, maxTimeout: time.Second, maxTransfer: 1024}
	observed.Env = server.prepareCommand(observed, []string{"PATH=/usr/bin:/bin", "HOME=/root"})
	out, err := observed.Output()
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(string(out)) != "65532" {
		t.Fatalf("command uid=%q", out)
	}
}
