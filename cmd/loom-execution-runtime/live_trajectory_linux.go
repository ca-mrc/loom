//go:build linux

package main

import (
	"fmt"
	"os"
	"syscall"
)

func openNativeTrajectoryFile(workspace, source string) (*os.File, os.FileInfo, error) {
	components, err := nativeTrajectoryComponents(source)
	if err != nil {
		return nil, nil, err
	}
	directory, err := syscall.Open(workspace, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = syscall.Close(directory) }()
	for _, component := range components[:len(components)-1] {
		next, err := syscall.Openat(directory, component, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
		if err != nil {
			return nil, nil, err
		}
		_ = syscall.Close(directory)
		directory = next
	}
	fd, err := syscall.Openat(directory, components[len(components)-1], syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, nil, err
	}
	file := os.NewFile(uintptr(fd), source)
	if file == nil {
		_ = syscall.Close(fd)
		return nil, nil, fmt.Errorf("trajectory open failed")
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		file.Close()
		return nil, nil, fmt.Errorf("trajectory is not regular")
	}
	return file, info, nil
}
