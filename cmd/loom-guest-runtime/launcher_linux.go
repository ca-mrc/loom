//go:build linux

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/qianyi-sun/loom/internal/guestchannel"
)

// All mutable guest state is private to one native sidecar incarnation. The
// enclosing Job lease supplies attempt/generation ownership and quota/cleanup.
// Values describe the complete outer sandbox limit, including QEMU overhead.
type guestConfig struct {
	Payload, Root, State, Socket     string
	MemoryMiB, StorageMiB, CPUMillis int
	MaxTransfer                      int64
	ExecTimeout                      int
	NestedDocker                     bool
	TaskIdentity                     bool
	TaskUID, TaskGID                 uint32
	TaskGroups                       []uint32
	TaskHome                         string
}

var (
	taskIDPattern   = regexp.MustCompile(`^(?:0|[1-9][0-9]*)$`)
	taskHomePattern = regexp.MustCompile(`^/(?:[-A-Za-z0-9._]+/)*[-A-Za-z0-9._]+$`)
)

func parseBoundedID(raw string) (uint32, error) {
	if !taskIDPattern.MatchString(raw) {
		return 0, errors.New("invalid guest task identity")
	}
	value, err := strconv.ParseUint(raw, 10, 32)
	if err != nil || value > 2147483647 {
		return 0, errors.New("invalid guest task identity")
	}
	return uint32(value), nil
}

func parseTaskGroups(raw string) ([]uint32, error) {
	if raw == "-" {
		return []uint32{}, nil
	}
	if len(raw) > 512 {
		return nil, errors.New("invalid guest task identity")
	}
	parts := strings.Split(raw, ",")
	if len(parts) > 32 {
		return nil, errors.New("invalid guest task identity")
	}
	groups := make([]uint32, 0, len(parts))
	var previous uint32
	for index, part := range parts {
		value, err := parseBoundedID(part)
		if err != nil {
			return nil, err
		}
		if index > 0 && value <= previous {
			return nil, errors.New("invalid guest task identity")
		}
		previous = value
		groups = append(groups, value)
	}
	return groups, nil
}

func (c *guestConfig) applyTaskIdentity(uid, gid, groups, home string) error {
	if uid == "" && gid == "" && groups == "" && home == "" {
		return nil
	}
	if uid == "" || gid == "" || groups == "" || home == "" {
		return errors.New("guest task identity flags must be set together")
	}
	parsedUID, err := parseBoundedID(uid)
	if err != nil {
		return err
	}
	parsedGID, err := parseBoundedID(gid)
	if err != nil {
		return err
	}
	parsedGroups, err := parseTaskGroups(groups)
	if err != nil {
		return err
	}
	c.TaskIdentity = true
	c.TaskUID = parsedUID
	c.TaskGID = parsedGID
	c.TaskGroups = parsedGroups
	c.TaskHome = home
	return nil
}

func validTaskHome(home string) bool {
	if len(home) > 4096 || !taskHomePattern.MatchString(home) || filepath.Clean(home) != home {
		return false
	}
	for _, part := range strings.Split(home, "/") {
		if part == "." || part == ".." {
			return false
		}
	}
	for _, protected := range []string{"/proc", "/sys", "/dev", "/run", "/loom", "/tests", "/verifier", "/solution"} {
		if home == protected || strings.HasPrefix(home, protected+"/") {
			return false
		}
	}
	return true
}

func (c guestConfig) validate() error {
	for _, p := range []string{c.Payload, c.Root, c.State, c.Socket} {
		if !filepath.IsAbs(p) || filepath.Clean(p) != p || strings.ContainsAny(p, ",\x00\r\n") {
			return errors.New("guest paths must be clean absolute paths without QEMU separators")
		}
	}
	if c.State == "/" || c.Payload == "/" || c.Socket == "/" {
		return errors.New("guest runtime paths cannot be filesystem roots")
	}
	if c.MemoryMiB < 512 || c.MemoryMiB > 1048576 || c.StorageMiB < 128 || c.StorageMiB > 1048576 || c.CPUMillis <= 0 || c.CPUMillis > 128000 || c.ExecTimeout <= 0 || c.ExecTimeout > 86400 || c.MaxTransfer <= 0 {
		return errors.New("guest resource or transfer bounds are invalid")
	}
	if c.TaskIdentity && !validTaskHome(c.TaskHome) {
		return errors.New("invalid guest task identity")
	}
	return nil
}

func (c guestConfig) qemuArgs() []string {
	// Guest page cache is anonymous QEMU memory from the outer cgroup's
	// perspective: host reclaim cannot evict it. Keep proportional room for
	// emulator/launcher allocations, host page tables and disk writeback even
	// when every guest RAM page has been touched. A fixed 256 MiB reserve OOMs
	// an 8 GiB sandbox during complete image-archive restore.
	overheadMiB := max(256, (c.MemoryMiB+7)/8)
	return []string{
		// QEMU's automatic translation cache can consume hundreds of MiB beyond
		// guest RAM. Pin its bound inside the reserved emulator envelope.
		"-L", filepath.Join(c.Payload, "share/qemu"), "-accel", "tcg,thread=multi,tb-size=64", "-machine", "q35", "-cpu", "max",
		"-smp", strconv.Itoa((c.CPUMillis + 999) / 1000), "-m", strconv.Itoa(c.MemoryMiB - overheadMiB),
		"-nographic", "-nodefaults", "-no-reboot", "-serial", "stdio", "-monitor", "none",
		"-bios", filepath.Join(c.Payload, "share/seabios/bios-256k.bin"), "-kernel", filepath.Join(c.Payload, "kernel"),
		"-initrd", filepath.Join(c.State, "initrd"), "-append", "console=ttyS0 panic=-1 rdinit=/init loom_guest=1 quiet",
		// Container mounts can reuse inode numbers. Remap the QID namespace so
		// guest caches cannot alias files from distinct outer filesystems.
		"-fsdev", "local,id=root,path=" + c.Root + ",security_model=none,readonly=on,multidevs=remap",
		"-device", "virtio-9p-pci,fsdev=root,mount_tag=taskroot",
		"-fsdev", "local,id=payload,path=" + c.Payload + ",security_model=none,readonly=on,multidevs=remap",
		"-device", "virtio-9p-pci,fsdev=payload,mount_tag=payload",
		"-drive", "file=" + filepath.Join(c.State, "state.ext4") + ",format=raw,if=virtio",
		"-device", "virtio-serial-pci", "-chardev", "socket,id=rpc,path=" + filepath.Join(c.State, "channel.sock") + ",server=on,wait=off",
		"-device", "virtserialport,chardev=rpc,name=loom.rpc",
		"-netdev", "user,id=net", "-device", "virtio-net-pci,netdev=net,romfile=",
	}
}

func (c guestConfig) prepareState() error {
	// Atomic mkdir is the incarnation fence. Even after graceful teardown the
	// directory remains as a tombstone until the owning Pod volume is deleted.
	// A Kubernetes sidecar restart must not resurrect a fresh task environment.
	if err := os.Mkdir(c.State, 0700); err != nil {
		return fmt.Errorf("guest state already exists or cannot be acquired: %w", err)
	}
	disk, err := os.OpenFile(filepath.Join(c.State, "state.ext4"), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	err = disk.Truncate(int64(c.StorageMiB) * 1024 * 1024)
	return errors.Join(err, disk.Close())
}

func (c guestConfig) tool(name string, args ...string) *exec.Cmd {
	loader := filepath.Join(c.Payload, "lib/ld-linux-x86-64.so.2")
	argv := []string{"--inhibit-cache", "--library-path", filepath.Join(c.Payload, "lib"), filepath.Join(c.Payload, "bin", name)}
	cmd := exec.Command(loader, append(argv, args...)...)
	// Never let image environment variables redirect the trusted loader/tools.
	cmd.Env = []string{"PATH=/usr/bin:/bin", "QEMU_MODULE_DIR=" + filepath.Join(c.Payload, "lib/qemu"), "MKE2FS_CONFIG=" + filepath.Join(c.Payload, "etc/mke2fs.conf")}
	return cmd
}

func runGuest(parent context.Context, c guestConfig) (failure error) {
	if err := c.validate(); err != nil {
		return err
	}
	if err := c.prepareState(); err != nil {
		return err
	}
	defer func() {
		// These paths were exclusively created by this invocation. Keep the state
		// directory and an explicit completion marker as the no-reuse fence.
		for _, name := range []string{"state.ext4", "initrd", "channel.sock"} {
			if err := os.Remove(filepath.Join(c.State, name)); err != nil && !os.IsNotExist(err) {
				failure = errors.Join(failure, err)
			}
		}
		failure = errors.Join(failure, os.WriteFile(filepath.Join(c.State, "retired"), []byte("guest incarnation retired; remove only with owning Pod\n"), 0600))
	}()
	formatTool := c.tool("mke2fs", "-q", "-t", "ext4", "-F", filepath.Join(c.State, "state.ext4"))
	// Bound provisioning independently of the guest's task command deadline.
	formatCtx, cancelFormat := context.WithTimeout(parent, 30*time.Second)
	format := exec.CommandContext(formatCtx, formatTool.Path, formatTool.Args[1:]...)
	format.Env = formatTool.Env
	format.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	formatErr := format.Run()
	cancelFormat()
	if formatErr != nil {
		return fmt.Errorf("guest disk preparation failed: %w", formatErr)
	}
	if err := c.writeInitramfs(); err != nil {
		return err
	}
	if err := parent.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	qemu := c.tool("qemu-system-x86_64", c.qemuArgs()...)
	qemu.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	qemu.Stdout, qemu.Stderr = os.Stdout, os.Stderr
	if err := qemu.Start(); err != nil {
		return fmt.Errorf("guest launch failed: %w", err)
	}
	stopped := make(chan struct{})
	var exitErr error
	go func() { exitErr = qemu.Wait(); close(stopped); cancel() }()
	defer func() {
		_ = qemu.Process.Kill()
		<-stopped
	}()
	// Includes kernel/bootstrap work and the guest's 60-second Docker budget.
	// Together with disk preparation this fits the 150-second startup probe.
	bootCtx, stopBoot := context.WithTimeout(ctx, 90*time.Second)
	channel, err := connectGuest(bootCtx, filepath.Join(c.State, "channel.sock"))
	stopBoot()
	if err != nil {
		return fmt.Errorf("guest bootstrap failed: %w", err)
	}
	defer channel.Close()
	listener, err := net.Listen("unix", c.Socket)
	if err != nil {
		return err
	}
	defer listener.Close()
	if err := os.Chmod(c.Socket, 0660); err != nil {
		return err
	}
	target := &url.URL{Scheme: "http", Host: "guest"}
	proxy := httputil.NewSingleHostReverseProxy(target)
	transport := &http.Transport{DialContext: channel.DialContext, MaxConnsPerHost: 128, MaxIdleConnsPerHost: 16}
	defer closeGuestTransport(channel, transport)
	proxy.Transport = transport
	proxy.ErrorHandler = func(w http.ResponseWriter, _ *http.Request, _ error) {
		http.Error(w, "guest unavailable", http.StatusBadGateway)
	}
	server := &http.Server{Handler: proxy, ReadHeaderTimeout: 5 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	defer server.Close()
	serving := make(chan error, 1)
	go func() { serving <- server.Serve(listener) }()
	select {
	case <-parent.Done():
		return parent.Err()
	case <-stopped:
		return fmt.Errorf("guest exited: %v", exitErr)
	case <-channel.Done():
		return errors.New("guest channel lost")
	case err := <-serving:
		return err
	}
}

func connectGuest(ctx context.Context, path string) (*guestchannel.Client, error) {
	for {
		conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
		if err == nil {
			return guestchannel.Connect(ctx, conn)
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if !errors.Is(err, syscall.ENOENT) && !errors.Is(err, syscall.ECONNREFUSED) {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}
