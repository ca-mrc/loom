//go:build !linux

package main

import (
	"fmt"
	"os"
	"path/filepath"
)

// Local developer platforms use component checks. Production Linux uses openat
// with O_NOFOLLOW for every component, anchored to the workspace descriptor.
func openNativeTrajectoryFile(workspace, source string) (*os.File, os.FileInfo, error) {
	components, err := nativeTrajectoryComponents(source)
	if err != nil {
		return nil, nil, err
	}
	if err := secureDirectory(workspace); err != nil {
		return nil, nil, err
	}
	current := workspace
	for _, component := range components[:len(components)-1] {
		current = filepath.Join(current, component)
		if err := secureDirectory(current); err != nil {
			return nil, nil, err
		}
	}
	file, info, err := openRegularOutputFile(filepath.Join(current, components[len(components)-1]))
	if err != nil {
		return nil, nil, err
	}
	after, err := os.Lstat(filepath.Join(current, components[len(components)-1]))
	if err != nil || !os.SameFile(info, after) {
		file.Close()
		return nil, nil, fmt.Errorf("trajectory identity changed")
	}
	return file, info, nil
}
