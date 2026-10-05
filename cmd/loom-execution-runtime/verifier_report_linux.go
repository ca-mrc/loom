//go:build linux

package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// prepareVerifierReport creates the direct harness's canonical report under
// the runtime identity. Pin every parent, including workspace ancestors, so
// neither creation nor replacement can follow a planted symlink.
func prepareVerifierReport(workspace, report string) error {
	if !filepath.IsAbs(workspace) || filepath.Clean(workspace) != workspace ||
		report != filepath.Join(workspace, ".loom", "verifier", "output.json") {
		return fmt.Errorf("verifier report must use the canonical workspace path")
	}
	flags := syscall.O_RDONLY | syscall.O_DIRECTORY | syscall.O_NOFOLLOW | syscall.O_CLOEXEC
	parent, err := syscall.Open("/", flags, 0)
	if err != nil {
		return err
	}
	defer func() { _ = syscall.Close(parent) }()
	for _, part := range strings.Split(strings.TrimPrefix(filepath.Dir(report), "/"), "/") {
		next, err := syscall.Openat(parent, part, flags, 0)
		if errors.Is(err, syscall.ENOENT) {
			err = syscall.Mkdirat(parent, part, 0o700)
			if err == nil || errors.Is(err, syscall.EEXIST) {
				next, err = syscall.Openat(parent, part, flags, 0)
			}
		}
		if err != nil {
			return err
		}
		_ = syscall.Close(parent)
		parent = next
	}
	const name = "output.json"
	previous, err := syscall.Openat(parent, name, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err == nil {
		var info syscall.Stat_t
		err = syscall.Fstat(previous, &info)
		_ = syscall.Close(previous)
		if err != nil || info.Mode&syscall.S_IFMT != syscall.S_IFREG {
			return fmt.Errorf("verifier report must be a regular file")
		}
	} else if !errors.Is(err, syscall.ENOENT) {
		return err
	}
	// Replacing the name, rather than truncating it, leaves any hardlink target
	// unchanged and clears stale scoring bytes. Empty output is not valid JSON.
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return err
	}
	temporary := ".loom-report-" + hex.EncodeToString(token[:])
	fd, err := syscall.Openat(parent, temporary, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0o600)
	if err != nil {
		return err
	}
	defer syscall.Unlinkat(parent, temporary)
	file := os.NewFile(uintptr(fd), temporary)
	if err := file.Close(); err != nil {
		return err
	}
	return syscall.Renameat(parent, temporary, parent, name)
}
