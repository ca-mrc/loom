//go:build linux

package main

import (
	"fmt"
	"io"
	"os"
	"syscall"
)

const prSetDumpable = 4

func hardenRuntimeIdentity() error {
	_, _, errno := syscall.Syscall6(syscall.SYS_PRCTL, prSetDumpable, 0, 0, 0, 0, 0)
	if errno != 0 {
		return fmt.Errorf("prctl(PR_SET_DUMPABLE): %w", errno)
	}
	return nil
}

func openRegularOutputFile(path string) (*os.File, os.FileInfo, error) {
	// A task may leave a FIFO at a declared artifact path. Open without
	// waiting for a writer, then validate the descriptor before reading it.
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = syscall.Close(fd)
		return nil, nil, fmt.Errorf("open output file")
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, nil, fmt.Errorf("output is not a regular file")
	}
	return file, info, nil
}

func readRegularOutputFile(path string) ([]byte, error) {
	file, _, err := openRegularOutputFile(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return io.ReadAll(file)
}
