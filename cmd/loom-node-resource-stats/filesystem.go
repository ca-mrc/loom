//go:build linux || darwin

package main

import (
	"context"
	"errors"
	"io"
	"math"
	"os"

	"golang.org/x/sys/unix"
)

const maxFilesystemEntries = 100000

var errFilesystemLimit = errors.New("filesystem_limit")

type inodeIdentity struct {
	device uint64
	inode  uint64
}
type diskWalker struct {
	seen                 map[inodeIdentity]bool
	entries              int
	total                uint64
	countDirectoryBlocks bool
}

// Every untrusted descendant is opened relative to a held directory descriptor.
// Never read file contents, follow symlinks, cross devices or select caller paths.
func openDirectory(root string, parts ...string) (*os.File, error) {
	fd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	for _, part := range parts {
		next, err := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		unix.Close(fd)
		if err != nil {
			return nil, err
		}
		fd = next
	}
	return os.NewFile(uintptr(fd), "resource-directory"), nil
}
func (w *diskWalker) add(stat *unix.Stat_t) error {
	w.entries++
	if w.entries > maxFilesystemEntries {
		return errFilesystemLimit
	}
	key := inodeIdentity{uint64(stat.Dev), uint64(stat.Ino)}
	if w.seen[key] {
		return nil
	}
	w.seen[key] = true
	if stat.Mode&unix.S_IFMT == unix.S_IFDIR && !w.countDirectoryBlocks {
		return nil
	}
	if stat.Blocks < 0 || uint64(stat.Blocks) > math.MaxUint64/512 || math.MaxUint64-w.total < uint64(stat.Blocks)*512 {
		return errFilesystemLimit
	}
	w.total += uint64(stat.Blocks) * 512
	return nil
}
func (w *diskWalker) tree(ctx context.Context, directory *os.File) error {
	return w.treeDepth(ctx, directory, 0)
}

func (w *diskWalker) treeDepth(ctx context.Context, directory *os.File, depth int) error {
	if depth > 256 {
		return errFilesystemLimit
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var root unix.Stat_t
	if err := unix.Fstat(int(directory.Fd()), &root); err != nil {
		return err
	}
	if err := w.add(&root); err != nil {
		return err
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		entries, err := directory.ReadDir(128)
		if err != nil && err != io.EOF {
			return err
		}
		for _, entry := range entries {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			var stat unix.Stat_t
			if err := unix.Fstatat(int(directory.Fd()), entry.Name(), &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
				return err
			}
			if uint64(stat.Dev) != uint64(root.Dev) {
				return errors.New("filesystem_mount")
			}
			if stat.Mode&unix.S_IFMT == unix.S_IFDIR {
				fd, err := unix.Openat(int(directory.Fd()), entry.Name(), unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
				if err != nil {
					return err
				}
				child := os.NewFile(uintptr(fd), "resource-directory")
				childErr := w.treeDepth(ctx, child, depth+1)
				child.Close()
				if childErr != nil {
					return childErr
				}
			} else if err := w.add(&stat); err != nil {
				return err
			}
		}
		if err == io.EOF {
			return nil
		}
	}
}
func newWalker() *diskWalker {
	return &diskWalker{seen: map[inodeIdentity]bool{}, countDirectoryBlocks: true}
}
func podDiskUsage(ctx context.Context, opts options, namespace string, pod podRequest) (uint64, string) {
	walker := newWalker()
	// Match kubelet log accounting: direct log files and every container log
	// directory's files, without charging the log-directory metadata itself.
	walker.countDirectoryBlocks = false
	logs, err := openDirectory(opts.podLogsRoot, namespace+"_"+pod.Name+"_"+pod.UID)
	if err != nil {
		return 0, "pod_logs_unavailable"
	}
	err = walker.tree(ctx, logs)
	logs.Close()
	walker.countDirectoryBlocks = true
	if err != nil {
		return 0, walkCode(ctx, err, "pod_logs_unavailable")
	}
	podDir, err := openDirectory(opts.kubeletPodsRoot, pod.UID)
	if err != nil {
		return 0, "pod_filesystem_unavailable"
	}
	defer podDir.Close()
	// The kubelet may omit etc-hosts when a Pod explicitly mounts /etc/hosts.
	// A confirmed absent file contributes zero; unreadable or linked files do not.
	var hosts unix.Stat_t
	err = unix.Fstatat(int(podDir.Fd()), "etc-hosts", &hosts, unix.AT_SYMLINK_NOFOLLOW)
	if err == nil {
		if hosts.Mode&unix.S_IFMT != unix.S_IFREG {
			return 0, "etc_hosts_unavailable"
		}
		if err := walker.add(&hosts); err != nil {
			return 0, "filesystem_limit"
		}
	} else if !errors.Is(err, unix.ENOENT) {
		return 0, "etc_hosts_unavailable"
	}
	for _, volume := range pod.Volumes {
		if ctx.Err() != nil {
			return 0, "observation_deadline"
		}
		if volume.Medium == "Memory" {
			continue
		}
		plugin := map[string]string{"": "kubernetes.io~empty-dir", "empty_dir": "kubernetes.io~empty-dir", "config_map": "kubernetes.io~configmap", "git_repo": "kubernetes.io~git-repo"}[volume.Kind]
		if plugin == "" {
			return 0, "volume_sample_unavailable"
		}
		directory, err := openDirectory(opts.kubeletPodsRoot, pod.UID, "volumes", plugin, volume.Name)
		if err != nil {
			return 0, "volume_sample_unavailable"
		}
		err = walker.tree(ctx, directory)
		directory.Close()
		if err != nil {
			return 0, walkCode(ctx, err, "volume_sample_unavailable")
		}
	}
	return walker.total, ""
}
func walkCode(ctx context.Context, err error, fallback string) string {
	if ctx.Err() != nil {
		return "observation_deadline"
	}
	if errors.Is(err, errFilesystemLimit) {
		return "filesystem_limit"
	}
	return fallback
}
func containerLogUsage(ctx context.Context, root, pod, container string) (uint64, error) {
	directory, err := openDirectory(root, pod, container)
	if err != nil {
		return 0, err
	}
	defer directory.Close()
	walker := newWalker()
	for {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		entries, err := directory.ReadDir(128)
		if err != nil && err != io.EOF {
			return 0, err
		}
		for _, entry := range entries {
			var stat unix.Stat_t
			if err := unix.Fstatat(int(directory.Fd()), entry.Name(), &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
				return 0, err
			}
			if stat.Mode&unix.S_IFMT == unix.S_IFDIR {
				continue
			}
			if stat.Mode&unix.S_IFMT != unix.S_IFREG {
				return 0, errors.New("invalid_log_file")
			}
			if err := walker.add(&stat); err != nil {
				return 0, err
			}
		}
		if err == io.EOF {
			break
		}
	}
	return walker.total, nil
}
