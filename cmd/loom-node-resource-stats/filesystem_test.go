//go:build linux || darwin

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func allocated(t *testing.T, path string) uint64 {
	t.Helper()
	var stat unix.Stat_t
	if err := unix.Lstat(path, &stat); err != nil {
		t.Fatal(err)
	}
	return uint64(stat.Blocks) * 512
}
func putFile(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, make([]byte, 8193), 0644); err != nil {
		t.Fatal(err)
	}
}
func TestPodDiskCountsVolumesAndLogsOnceWithoutFollowingLinks(t *testing.T) {
	opts := fixture(t)
	logPod := filepath.Join(opts.podLogsRoot, "execution_job-pod_"+podUID)
	container := filepath.Join(logPod, "execution")
	first := filepath.Join(container, "0.log")
	rotated := filepath.Join(container, "0.log.20261031")
	previous := filepath.Join(container, "1.log")
	if err := os.Link(first, rotated); err != nil {
		t.Fatal(err)
	}
	putFile(t, previous)
	// Only inode metadata is inspected; a FIFO must never block on open/read.
	fifo := filepath.Join(container, "named-pipe")
	if err := unix.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	putFile(t, filepath.Join(outside, "not-pod-data"))
	link := filepath.Join(logPod, "outside-link")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	workspace := filepath.Join(opts.kubeletPodsRoot, podUID, "volumes", "kubernetes.io~empty-dir", "workspace")
	data := filepath.Join(workspace, "work")
	putFile(t, data)
	hosts := filepath.Join(opts.kubeletPodsRoot, podUID, "etc-hosts")
	putFile(t, hosts)
	pod := podRequest{UID: podUID, Name: "job-pod", Volumes: []volumeRequest{{Name: "workspace"}, {Name: "memory", Medium: "Memory"}}}
	used, code := podDiskUsage(context.Background(), opts, "execution", pod)
	want := allocated(t, first) + allocated(t, previous) + allocated(t, fifo) + allocated(t, link) + allocated(t, workspace) + allocated(t, data) + allocated(t, hosts)
	if code != "" || used != want {
		t.Fatalf("wrong disk composition: got %d/%s want %d", used, code, want)
	}
	// Container log reporting deliberately includes every attempt and rotation,
	// as kubelet does. A special file is unknown, not read or invented as zero.
	if _, err := containerLogUsage(context.Background(), opts.podLogsRoot, filepath.Base(logPod), "execution"); err == nil {
		t.Fatal("FIFO was reported as log bytes")
	}
	if err := os.Remove(fifo); err != nil {
		t.Fatal(err)
	}
	logs, err := containerLogUsage(context.Background(), opts.podLogsRoot, filepath.Base(logPod), "execution")
	if err != nil || logs != allocated(t, first)+allocated(t, previous) {
		t.Fatalf("container logs duplicated rotations: %d %v", logs, err)
	}
}

func TestMissingVolumeAndLinkedAncestorsDoNotYieldPodTotal(t *testing.T) {
	opts := fixture(t)
	pod := podRequest{UID: podUID, Name: "job-pod", Volumes: []volumeRequest{{Name: "workspace"}}}
	if _, code := podDiskUsage(context.Background(), opts, "execution", pod); code != "volume_sample_unavailable" {
		t.Fatalf("missing volume counted: %s", code)
	}
	directory := filepath.Join(opts.kubeletPodsRoot, podUID, "volumes", "kubernetes.io~empty-dir")
	if err := os.MkdirAll(directory, 0755); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	putFile(t, filepath.Join(outside, "private"))
	if err := os.Symlink(outside, filepath.Join(directory, "workspace")); err != nil {
		t.Fatal(err)
	}
	if _, code := podDiskUsage(context.Background(), opts, "execution", pod); code != "volume_sample_unavailable" {
		t.Fatalf("linked volume escaped root: %s", code)
	}
	if err := os.Symlink(filepath.Join(outside, "private"), filepath.Join(opts.kubeletPodsRoot, podUID, "etc-hosts")); err != nil {
		t.Fatal(err)
	}
	if _, code := podDiskUsage(context.Background(), opts, "execution", pod); code != "etc_hosts_unavailable" {
		t.Fatalf("linked etc-hosts escaped root: %s", code)
	}
}

func TestFilesystemWalkDeadlineAndEntryBound(t *testing.T) {
	opts := fixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, code := podDiskUsage(ctx, opts, "execution", podRequest{UID: podUID, Name: "job-pod"}); code != "observation_deadline" {
		t.Fatalf("expired walker continued: %s", code)
	}
	dir, err := openDirectory(opts.podLogsRoot, "execution_job-pod_"+podUID)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	walker := newWalker()
	walker.entries = maxFilesystemEntries
	if err := walker.tree(context.Background(), dir); err != errFilesystemLimit {
		t.Fatalf("entry bound ignored: %v", err)
	}
}

func TestConfigMapAndGitRepoVolumesUseFixedPluginPaths(t *testing.T) {
	opts := fixture(t)
	pod := podRequest{UID: podUID, Name: "job-pod", Volumes: []volumeRequest{{Kind: "config_map", Name: "config"}, {Kind: "git_repo", Name: "source"}}}
	config := filepath.Join(opts.kubeletPodsRoot, podUID, "volumes", "kubernetes.io~configmap", "config")
	git := filepath.Join(opts.kubeletPodsRoot, podUID, "volumes", "kubernetes.io~git-repo", "source")
	dataDir := filepath.Join(config, "..2026_10_31")
	putFile(t, filepath.Join(dataDir, "value"))
	if err := os.Symlink("..2026_10_31", filepath.Join(config, "..data")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("..data/value", filepath.Join(config, "value")); err != nil {
		t.Fatal(err)
	}
	putFile(t, filepath.Join(git, "README"))
	result, code := podDiskUsage(context.Background(), opts, "execution", pod)
	if code != "" {
		t.Fatalf("local ephemeral plugin omitted: %s", code)
	}
	noVolumes, _ := podDiskUsage(context.Background(), opts, "execution", podRequest{UID: podUID, Name: "job-pod"})
	expected := allocated(t, config) + allocated(t, dataDir) + allocated(t, filepath.Join(dataDir, "value")) + allocated(t, filepath.Join(config, "..data")) + allocated(t, filepath.Join(config, "value")) + allocated(t, git) + allocated(t, filepath.Join(git, "README"))
	if result != noVolumes+expected {
		t.Fatalf("config map symlinks double-counted data: got %d want %d", result, noVolumes+expected)
	}
	pod.Volumes[1].Kind = "unsupported"
	if _, code := podDiskUsage(context.Background(), opts, "execution", pod); code != "volume_sample_unavailable" {
		t.Fatalf("unknown volume silently omitted: %s", code)
	}
}
