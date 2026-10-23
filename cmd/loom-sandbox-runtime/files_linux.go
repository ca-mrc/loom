//go:build linux

package main

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"strings"
	"syscall"
	"unsafe"
)

// Every directory is pinned by descriptor before traversing the next component.
// O_NOFOLLOW on all components prevents task-controlled symlink races.
func fileParent(path string, create bool) (int, string, error) {
	return openFileParent(path, create, syscall.O_RDONLY)
}

func openFileParent(path string, create bool, directoryFlags int) (int, string, error) {
	if !strings.HasPrefix(path, "/") || strings.ContainsRune(path, '\x00') {
		return -1, "", errInvalidFileTarget
	}
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for _, p := range parts {
		if p == "" || p == "." || p == ".." {
			return -1, "", errInvalidFileTarget
		}
	}
	fd, err := syscall.Open("/", directoryFlags|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return -1, "", err
	}
	for _, part := range parts[:len(parts)-1] {
		next, err := syscall.Openat(fd, part, directoryFlags|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
		if create && errors.Is(err, syscall.ENOENT) {
			err = syscall.Mkdirat(fd, part, 0755)
			if err == nil || errors.Is(err, syscall.EEXIST) {
				next, err = syscall.Openat(fd, part, directoryFlags|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
			}
		}
		syscall.Close(fd)
		if err != nil {
			return -1, "", err
		}
		fd = next
	}
	return fd, parts[len(parts)-1], nil
}

func readFile(path string) (*os.File, error) {
	dir, name, err := fileParent(path, false)
	if err != nil {
		return nil, err
	}
	defer syscall.Close(dir)
	fd, err := syscall.Openat(dir, name, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), name)
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		f.Close()
		return nil, errInvalidFileTarget
	}
	return f, nil
}

func sameFileReadVersion(before, after os.FileInfo) bool {
	b, bok := before.Sys().(*syscall.Stat_t)
	a, aok := after.Sys().(*syscall.Stat_t)
	return bok && aok && os.SameFile(before, after) && before.Size() == after.Size() &&
		before.ModTime() == after.ModTime() && b.Ctim == a.Ctim
}

// Read only the literal link text. Pin its parent using the same no-follow
// traversal as file transfers; readlinkat never dereferences the leaf.
func readSymlink(path string) (string, error) {
	dir, name, err := fileParent(path, false)
	if err != nil {
		return "", err
	}
	defer syscall.Close(dir)
	leaf, err := syscall.BytePtrFromString(name)
	if err != nil {
		return "", err
	}
	buf := make([]byte, 4097)
	n, _, errno := syscall.Syscall6(syscall.SYS_READLINKAT, uintptr(dir),
		uintptr(unsafe.Pointer(leaf)), uintptr(unsafe.Pointer(&buf[0])), uintptr(len(buf)), 0, 0)
	if errno != 0 {
		return "", errno
	}
	if n == 0 || n > 4096 {
		return "", errors.New("symlink target exceeds limit")
	}
	return string(buf[:n]), nil
}

func writeFile(path string, body io.Reader, mode uint32) error {
	dir, name, err := fileParent(path, true)
	if err != nil {
		return err
	}
	defer syscall.Close(dir)
	// Reject an existing link or special target. Atomic rename below also avoids
	// following a link introduced after this check.
	existing, err := syscall.Openat(dir, name, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if err == nil {
		f := os.NewFile(uintptr(existing), name)
		info, statErr := f.Stat()
		f.Close()
		if statErr != nil || !info.Mode().IsRegular() {
			return errInvalidFileTarget
		}
	} else if !errors.Is(err, syscall.ENOENT) {
		return err
	}
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return err
	}
	temporary := ".loom-upload-" + hex.EncodeToString(token[:])
	fd, err := syscall.Openat(dir, temporary, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_EXCL|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, mode)
	if err != nil {
		return err
	}
	defer syscall.Unlinkat(dir, temporary)
	f := os.NewFile(uintptr(fd), temporary)
	_, err = io.Copy(f, body)
	if err == nil {
		err = f.Chmod(os.FileMode(mode))
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return syscall.Renameat(dir, temporary, dir, name)
}
