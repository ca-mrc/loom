package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

// materializeGuestPayload copies only regular files from the admitted runtime
// image into a new runtime volume directory. A bounded streaming copy avoids
// loading the kernel/module bundle into the materializer's memory allocation.
func materializeGuestPayload(source, destination string, maximumBytes int64) (failure error) {
	if maximumBytes <= 0 || !filepath.IsAbs(source) || !filepath.IsAbs(destination) || filepath.Clean(source) != source || filepath.Clean(destination) != destination {
		return fmt.Errorf("invalid guest payload path or budget")
	}
	info, err := os.Lstat(source)
	if err != nil || !info.IsDir() {
		return fmt.Errorf("guest payload source is not a directory")
	}
	if err := secureDirectory(filepath.Dir(destination)); err != nil {
		return err
	}
	if err := os.Mkdir(destination, 0755); err != nil {
		return err
	}
	defer func() {
		if failure != nil {
			failure = errors.Join(failure, os.RemoveAll(destination))
		}
	}()
	remaining := maximumBytes
	return filepath.WalkDir(source, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == source {
			return nil
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			return os.Mkdir(target, 0755)
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("guest payload contains a non-regular file")
		}
		if info.Size() > remaining {
			return fmt.Errorf("guest payload exceeds runtime volume budget")
		}
		mode := os.FileMode(0444)
		if info.Mode()&0111 != 0 {
			mode = 0555
		}
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		defer input.Close()
		output, err := os.OpenFile(target, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
		if err != nil {
			return err
		}
		copied, copyErr := io.Copy(output, io.LimitReader(input, remaining+1))
		closeErr := output.Close()
		if copyErr != nil || closeErr != nil {
			return errors.Join(copyErr, closeErr)
		}
		if copied != info.Size() || copied > remaining {
			return fmt.Errorf("guest payload size changed during copy")
		}
		remaining -= copied
		return nil
	})
}
