//go:build linux

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func configForTest(t *testing.T) guestConfig {
	t.Helper()
	root := t.TempDir()
	return guestConfig{Payload: filepath.Join(root, "payload"), Root: filepath.Join(root, "root"), State: filepath.Join(root, "state"), Socket: filepath.Join(root, "sandbox.sock"), MemoryMiB: 1024, StorageMiB: 512, CPUMillis: 1000, MaxTransfer: 64 << 20, ExecTimeout: 60}
}

func TestGuestConfigurationRejectsUnsafePathsAndUnboundedResources(t *testing.T) {
	for _, change := range []func(*guestConfig){
		func(c *guestConfig) { c.State = "/" }, func(c *guestConfig) { c.Payload = "relative" },
		func(c *guestConfig) { c.Root = "/safe,readonly=off" }, func(c *guestConfig) { c.Socket = "/tmp/../x" },
		func(c *guestConfig) { c.MemoryMiB = 255 }, func(c *guestConfig) { c.StorageMiB = 0 },
		func(c *guestConfig) { c.CPUMillis = 0 }, func(c *guestConfig) { c.CPUMillis = 128001 },
		func(c *guestConfig) { c.MaxTransfer = 0 }, func(c *guestConfig) { c.ExecTimeout = 86401 },
	} {
		c := configForTest(t)
		change(&c)
		if c.validate() == nil {
			t.Fatalf("unsafe guest configuration accepted: %#v", c)
		}
	}
	if err := configForTest(t).validate(); err != nil {
		t.Fatal(err)
	}
}

func TestQEMUUsesSoftwareGuestAndReadOnlyExports(t *testing.T) {
	c := configForTest(t)
	args := strings.Join(c.qemuArgs(), " ")
	for _, expected := range []string{"tcg,thread=multi", "-nodefaults", "-no-reboot", "readonly=on", "-monitor none", "-smp 1", "-m 768", "mount_tag=taskroot", "mount_tag=payload", "virtserialport", "-netdev user,id=net"} {
		if !strings.Contains(args, expected) {
			t.Errorf("missing %s in %s", expected, args)
		}
	}
	for _, forbidden := range []string{"enable-kvm", "/dev/kvm", "hostfwd", "host=", "/var/run/docker.sock", "vhost="} {
		if strings.Contains(args, forbidden) {
			t.Errorf("unexpected host access: %s", args)
		}
	}
}

func TestGuestStateNeverReusesExistingDirectory(t *testing.T) {
	c := configForTest(t)
	if err := os.Mkdir(c.State, 0700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(c.State, "foreign")
	if err := os.WriteFile(marker, []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := c.prepareState(); err == nil {
		t.Fatal("reused prior state")
	}
	if contents, err := os.ReadFile(marker); err != nil || string(contents) != "preserve" {
		t.Fatalf("foreign state changed: %s %v", contents, err)
	}
}
