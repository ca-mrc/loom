//go:build linux

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// During early boot the overlay lives at /root, before chroot. Resolve task
// image links with guest-root semantics: /var/run -> /run names /root/run,
// not the initramfs /run. Missing suffixes are safe to create beneath that root.
func guestRootPath(root, path string) (string, error) {
	pending := strings.Split(strings.TrimPrefix(filepath.Clean("/"+path), "/"), "/")
	resolved := "/"
	links := 0
	for len(pending) > 0 {
		part := pending[0]
		pending = pending[1:]
		next := filepath.Join(resolved, part)
		info, err := os.Lstat(filepath.Join(root, next))
		if os.IsNotExist(err) {
			resolved = next
			continue
		}
		if err != nil {
			return "", err
		}
		if info.Mode()&os.ModeSymlink == 0 {
			resolved = next
			continue
		}
		links++
		if links > 40 {
			return "", fmt.Errorf("guest root contains a symlink loop")
		}
		target, err := os.Readlink(filepath.Join(root, next))
		if err != nil {
			return "", err
		}
		if !filepath.IsAbs(target) {
			target = filepath.Join(resolved, target)
		}
		pending = append(strings.Split(strings.TrimPrefix(filepath.Clean(target), "/"), "/"), pending...)
		resolved = "/"
	}
	return filepath.Join(root, resolved), nil
}
