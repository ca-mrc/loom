//go:build !linux

package main

import (
	"context"
	"errors"
	"io"
	"os"
)

func stopProcesses(context.Context) error {
	return errCleanupPIDNamespace
}

func readFile(string) (*os.File, error) {
	return nil, errors.New("secure file transfers require Linux")
}

func sameFileReadVersion(before, after os.FileInfo) bool {
	return os.SameFile(before, after) && before.Size() == after.Size() && before.ModTime() == after.ModTime()
}
func readSymlink(string) (string, error) {
	return "", errors.New("secure symlink inspection requires Linux")
}
func writeFile(string, io.Reader, uint32) error {
	return errors.New("secure file transfers require Linux")
}

func replaceDirectory(string, string) error {
	return errors.New("secure directory restoration requires Linux")
}
