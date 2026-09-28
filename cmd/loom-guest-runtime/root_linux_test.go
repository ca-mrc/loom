//go:build linux

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGuestRootPathsResolveTaskSymlinksWithinGuest(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "var"), 0755); err != nil {
		t.Fatal(err)
	}
	for name, target := range map[string]string{"var/run": "/run", "lib": "usr/lib", "escape": "../../etc", "loop": "/loop"} {
		if err := os.Symlink(target, filepath.Join(root, name)); err != nil {
			t.Fatal(err)
		}
	}
	for name, want := range map[string]string{"/var/run": "run", "/lib/modules": "usr/lib/modules", "/escape/private": "etc/private"} {
		got, err := guestRootPath(root, name)
		if err != nil || got != filepath.Join(root, want) {
			t.Fatalf("%s: %s %v", name, got, err)
		}
	}
	if _, err := guestRootPath(root, "/loop/file"); err == nil {
		t.Fatal("accepted symlink loop")
	}
}
