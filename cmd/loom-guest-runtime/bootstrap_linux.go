//go:build linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

const guestPayload = "/loom/guest-tools"
const guestConfigPath = "/loom/guest-config.json"

func bootMust(err error) {
	if err != nil {
		panic(err)
	}
}

func guestInit() {
	// This branch only runs as the initramfs PID 1; a host invocation must never
	// reach mount, module-loading, reboot or process-namespace operations.
	if os.Getpid() != 1 {
		panic("guest bootstrap requires PID 1")
	}
	defer func() {
		if value := recover(); value != nil {
			fmt.Fprintln(os.Stderr, "guest bootstrap failed:", value)
		}
		syscall.Sync()
		_ = syscall.Reboot(syscall.LINUX_REBOOT_CMD_POWER_OFF)
	}()
	bootMust(syscall.Mount("proc", "/proc", "proc", 0, ""))
	bootMust(requireGuestKernel())
	bootMust(syscall.Mount("sysfs", "/sys", "sysfs", 0, ""))
	bootMust(syscall.Mount("devtmpfs", "/dev", "devtmpfs", 0, ""))
	for _, name := range []string{"netfs", "9pnet", "9pnet_virtio", "9p", "overlay"} {
		bootMust(loadModule("/modules/" + name + ".ko"))
	}
	bootMust(syscall.Mount("taskroot", "/lower", "9p", syscall.MS_RDONLY, "trans=virtio,version=9p2000.L,ro,cache=loose,msize=1048576"))
	bootMust(syscall.Mount("payload", "/payload", "9p", syscall.MS_RDONLY, "trans=virtio,version=9p2000.L,ro,cache=loose,msize=1048576"))
	bootMust(syscall.Mount("/dev/vda", "/state", "ext4", 0, ""))
	for _, name := range []string{"upper", "work", "docker"} {
		bootMust(os.MkdirAll("/state/"+name, 0755))
	}
	bootMust(syscall.Mount("overlay", "/root", "overlay", 0, "lowerdir=/lower,upperdir=/state/upper,workdir=/state/work"))
	for _, name := range []string{"proc", "sys", "dev", "dev/pts", "sys/fs/cgroup", "run", "var/run", "loom/guest-tools", "lib/modules", "var/lib/docker", "tmp"} {
		target, err := guestRootPath("/root", name)
		bootMust(err)
		bootMust(os.MkdirAll(target, 0755))
	}
	for _, mount := range [][2]string{{"/payload", "/root" + guestPayload}, {"/payload/modules", "/root/lib/modules"}, {"/dev", "/root/dev"}, {"/sys", "/root/sys"}, {"/state/docker", "/root/var/lib/docker"}} {
		target, err := guestRootPath("/root", strings.TrimPrefix(mount[1], "/root"))
		bootMust(err)
		bootMust(syscall.Mount(mount[0], target, "", syscall.MS_BIND|syscall.MS_REC, ""))
	}
	raw, err := os.ReadFile("/config.json")
	bootMust(err)
	configPath, err := guestRootPath("/root", guestConfigPath)
	bootMust(err)
	bootMust(os.WriteFile(configPath, raw, 0600))
	// Switch the guest's initial mount namespace to the task root too. Kernel
	// usermode helpers (notably module autoload) must see the same module tree
	// and helper path as the sandbox, rather than the discarded initramfs.
	bootMust(syscall.Chdir("/root"))
	bootMust(syscall.Mount(".", "/", "", syscall.MS_MOVE, ""))
	bootMust(syscall.Chroot("."))
	bootMust(syscall.Chdir("/"))
	bootMust(syscall.Mount("proc", "/proc", "proc", 0, ""))
	// A separate PID namespace excludes guest kernel threads from the sandbox's
	// existing pause/stop descendant walk. Its PID1 still owns all task daemons.
	cmd := exec.Command(guestPayload+"/bin/loom-guest-runtime", "--sandbox-init")
	cmd.Env = []string{"PATH=/usr/bin:/bin"}
	cmd.SysProcAttr = &syscall.SysProcAttr{Cloneflags: syscall.CLONE_NEWPID | syscall.CLONE_NEWNS}
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	bootMust(cmd.Run())
}

func requireGuestKernel() error {
	data, err := os.ReadFile("/proc/cmdline")
	if err != nil {
		return err
	}
	for _, field := range strings.Fields(string(data)) {
		if field == "loom_guest=1" {
			return nil
		}
	}
	return errors.New("operation requires the Loom guest kernel")
}

func loadModule(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return errors.New("empty guest module")
	}
	empty, err := syscall.BytePtrFromString("")
	if err != nil {
		return err
	}
	_, _, errno := syscall.Syscall(syscall.SYS_INIT_MODULE, uintptr(unsafe.Pointer(&data[0])), uintptr(len(data)), uintptr(unsafe.Pointer(empty)))
	runtime.KeepAlive(data)
	if errno != 0 {
		return fmt.Errorf("load guest module %s: %w", filepath.Base(path), errno)
	}
	return nil
}

func sandboxInit() {
	if os.Getpid() != 1 {
		panic("guest sandbox requires PID 1")
	}
	// The original guest proc mount still describes guest-global processes.
	// Replace it before running the existing namespace-scoped sandbox server.
	bootMust(syscall.Mount("proc", "/proc", "proc", 0, ""))
	bootMust(requireGuestKernel())
	bootMust(os.MkdirAll("/sys/fs/cgroup", 0755))
	bootMust(syscall.Mount("cgroup2", "/sys/fs/cgroup", "cgroup2", 0, ""))
	bootMust(os.MkdirAll("/dev/pts", 0755))
	bootMust(syscall.Mount("devpts", "/dev/pts", "devpts", 0, "newinstance,ptmxmode=0666,mode=0620"))
	bootMust(syscall.Mount("/dev/pts/ptmx", "/dev/ptmx", "", syscall.MS_BIND, ""))
	for _, link := range [][2]string{{"/proc/self/fd", "/dev/fd"}, {"/proc/self/fd/0", "/dev/stdin"}, {"/proc/self/fd/1", "/dev/stdout"}, {"/proc/self/fd/2", "/dev/stderr"}} {
		if err := os.Symlink(link[0], link[1]); err != nil && !os.IsExist(err) {
			bootMust(err)
		}
	}
	bootMust(syscall.Setrlimit(syscall.RLIMIT_CORE, &syscall.Rlimit{Cur: ^uint64(0), Max: ^uint64(0)}))
	bootMust(syscall.Setrlimit(syscall.RLIMIT_NOFILE, &syscall.Rlimit{Cur: 1048576, Max: 1048576}))
	raw, err := os.ReadFile(guestConfigPath)
	bootMust(err)
	var config guestBootConfig
	bootMust(json.Unmarshal(raw, &config))
	if config.MaxTransfer <= 0 || config.ExecTimeout <= 0 {
		panic("invalid guest command bounds")
	}
	bootMust(installGuestTools())
	bootMust(configureGuestNetwork())
	// Kernel requests for overlay/loop/netfilter dependencies are also confined
	// to the guest. Never use a task-image modprobe compiled for another kernel.
	bootMust(os.WriteFile("/proc/sys/kernel/modprobe", []byte("/loom/guest-wrappers/modprobe\n"), 0644))
	if config.NestedDocker {
		// Task images can already own Docker plugin directories or symlinks.
		// Select our bundled plugin through a private client configuration instead
		// of mutating those paths.
		bootMust(os.MkdirAll("/loom/docker-client", 0700))
		bootMust(os.WriteFile("/loom/docker-client/config.json", []byte(`{"cliPluginsExtraDirs":["/loom/guest-tools/docker/cli-plugins"]}`), 0600))
		config.Environment = append(config.Environment, "DOCKER_CONFIG=/loom/docker-client")
	}
	environment := guestEnvironment(config.Environment)
	if config.NestedDocker {
		bootMust(startDocker(environment))
	}
	binary := guestPayload + "/bin/loom-sandbox-runtime"
	bootMust(syscall.Exec(binary, []string{binary, "--guest-channel", "--max-transfer-bytes", strconv.FormatInt(config.MaxTransfer, 10), "--exec-timeout-seconds", strconv.Itoa(config.ExecTimeout)}, environment))
}

func configureGuestNetwork() error {
	busybox := guestPayload + "/bin/busybox"
	entries, err := os.ReadDir("/sys/class/net")
	if err != nil {
		return err
	}
	device := ""
	for _, entry := range entries {
		if entry.Name() != "lo" {
			if device != "" {
				return errors.New("unexpected guest network interfaces")
			}
			device = entry.Name()
		}
	}
	if device == "" {
		return errors.New("guest network interface missing")
	}
	for _, args := range [][]string{{"ip", "link", "set", "lo", "up"}, {"ip", "link", "set", device, "up"}, {"ip", "addr", "add", "10.0.2.15/24", "dev", device}, {"ip", "route", "add", "default", "via", "10.0.2.2"}} {
		if output, err := exec.Command(busybox, args...).CombinedOutput(); err != nil {
			return fmt.Errorf("guest network setup: %w: %s", err, output)
		}
	}
	// user-mode networking forwards DNS to the outer Pod's resolver and inherits
	// its NetworkPolicy. It creates no externally listening host port.
	return os.WriteFile("/etc/resolv.conf", []byte("nameserver 10.0.2.3\n"), 0644)
}

func installGuestTools() error {
	// Payload is read-only; wrappers live on the guest's own bounded writable
	// disk. Keep absolute loader/library paths independent of task image ABI.
	if err := os.MkdirAll("/loom/guest-wrappers", 0755); err != nil {
		return err
	}
	for _, name := range []string{"modprobe", "iptables", "ip6tables", "iptables-save", "ip6tables-save", "iptables-restore", "ip6tables-restore"} {
		binary := "xtables-legacy-multi"
		if name == "modprobe" {
			binary = "kmod"
		}
		body := "#!/bin/sh\nexport XTABLES_LIBDIR=" + guestPayload + "/lib/xtables\nexec " + guestPayload + "/lib/ld-linux-x86-64.so.2 --library-path " + guestPayload + "/lib --argv0 " + name + " " + guestPayload + "/bin/" + binary + " \"$@\"\n"
		if err := os.WriteFile("/loom/guest-wrappers/"+name, []byte(body), 0755); err != nil {
			return err
		}
	}
	return nil
}

func guestEnvironment(original []string) []string {
	values := map[string]string{}
	for _, entry := range original {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			values[key] = value
		}
	}
	path := values["PATH"]
	if path == "" {
		path = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
	}
	values["PATH"] = "/loom/guest-wrappers:" + guestPayload + "/docker:" + guestPayload + "/bin:" + path
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, key+"="+values[key])
	}
	return result
}

func startDocker(environment []string) error {
	if err := os.MkdirAll("/var/run", 0755); err != nil {
		return err
	}
	daemon := exec.Command(guestPayload+"/docker/dockerd", "--host=unix:///var/run/docker.sock", "--data-root=/var/lib/docker", "--exec-root=/run/docker", "--storage-driver=overlay2", "--pidfile=/run/docker.pid")
	daemon.Env = append(append([]string(nil), environment...),
		"HTTP_PROXY=http://10.0.2.2:18791", "HTTPS_PROXY=http://10.0.2.2:18791",
		"NO_PROXY=localhost,127.0.0.1,::1")
	daemon.Stdout, daemon.Stderr = os.Stdout, os.Stderr
	if err := daemon.Start(); err != nil {
		return err
	}
	exited := make(chan error, 1)
	go func() { exited <- daemon.Wait() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return waitDocker(ctx, exited, "/var/run/docker.sock")
}

func waitDocker(ctx context.Context, exited <-chan error, socket string) error {
	// Repeatedly starting the CLI under TCG competes with daemon boot for the
	// guest's CPU. Probe its private API directly under the same startup budget.
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	for {
		request, err := http.NewRequestWithContext(ctx, http.MethodHead, "http://docker/_ping", nil)
		if err != nil {
			return err
		}
		response, err := client.Do(request)
		if err == nil {
			response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("guest Docker daemon did not become ready: %w", ctx.Err())
		case err := <-exited:
			return fmt.Errorf("guest Docker daemon exited: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
}
