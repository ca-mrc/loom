// loom-sandbox-runtime exposes only the container's execution and file surface.
// Its Unix socket is mounted into the trusted agent, never another sandbox.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/qianyi-sun/loom/internal/guestchannel"
)

const maxOutput = 10 * 1024 * 1024

var (
	errCleanupPIDNamespace = errors.New("sandbox runtime must be Linux PID 1")
	errCleanupProcessOwner = errors.New("unexpected sandbox process owner")
	errCleanupProcRead     = errors.New("cannot inspect sandbox processes")
	errCleanupSignalDenied = errors.New("sandbox cannot signal descendant")
	errCleanupSignalFailed = errors.New("sandbox signal failed")
)

// Kernel identity for one descendant that cleanup could not reap. Command
// lines, environments and file paths stay out of this value.
type processSnapshot struct {
	PID           int
	ParentPID     uint64
	State         string
	UID           uint64
	WaitChannel   string
	KillErrno     int
	SignalPending bool
}

type cleanupDiagnosticError struct {
	cause     error
	snapshots []processSnapshot
}

func (e *cleanupDiagnosticError) Error() string { return e.cause.Error() }
func (e *cleanupDiagnosticError) Unwrap() error { return e.cause }

var waitChannelPattern = regexp.MustCompile(`^[A-Za-z0-9_.]{1,64}$`)

func (snapshot processSnapshot) diagnostic() (string, bool) {
	if snapshot.PID <= 1 || snapshot.PID > 1<<31-1 || snapshot.ParentPID > 1<<31-1 || snapshot.UID > 1<<32-1 {
		return "", false
	}
	if len(snapshot.State) != 1 || !strings.Contains("RSDTtXZPIUW", snapshot.State) {
		return "", false
	}
	if snapshot.KillErrno < 0 || snapshot.KillErrno > 255 {
		return "", false
	}
	wait := snapshot.WaitChannel
	if !waitChannelPattern.MatchString(wait) {
		wait = "unavailable"
	}
	pending := 0
	if snapshot.SignalPending {
		pending = 1
	}
	return fmt.Sprintf(
		"pid=%d;ppid=%d;state=%s;uid=%d;wchan=%s;kill_errno=%d;sigkill_pending=%d",
		snapshot.PID, snapshot.ParentPID, snapshot.State, snapshot.UID, wait, snapshot.KillErrno, pending,
	), true
}

func cleanupProcessHeader(snapshots []processSnapshot) string {
	parts := make([]string, 0, 4)
	for _, snapshot := range snapshots {
		if len(parts) == 4 {
			break
		}
		part, ok := snapshot.diagnostic()
		if ok {
			parts = append(parts, part)
		}
	}
	return strings.Join(parts, ",")
}

// Only bounded kernel identity fields may cross the cleanup error boundary.
// Command lines, environment and paths must never be attached to this error.
type processOwnerError struct {
	PID         int
	ParentPID   uint64
	State       string
	ExpectedUID int
	ObservedUID uint64
}

func (e *processOwnerError) Error() string { return errCleanupProcessOwner.Error() }
func (e *processOwnerError) Unwrap() error { return errCleanupProcessOwner }

// These fixed codes carry no process, command, environment, or filesystem data.
func cleanupErrorCode(err error) string {
	switch {
	case errors.Is(err, errCleanupPIDNamespace):
		return "pid_namespace_invalid"
	case errors.Is(err, errCleanupProcessOwner):
		return "process_owner_mismatch"
	case errors.Is(err, errCleanupProcRead):
		return "process_inspection_failed"
	case errors.Is(err, errCleanupSignalDenied):
		return "cleanup_signal_denied"
	case errors.Is(err, errCleanupSignalFailed):
		return "cleanup_signal_failed"
	case errors.Is(err, context.DeadlineExceeded):
		return "cleanup_timeout"
	case errors.Is(err, context.Canceled):
		return "cleanup_cancelled"
	default:
		return "cleanup_failed"
	}
}

func writeCleanupFailure(w http.ResponseWriter, err error) {
	w.Header().Set("X-Loom-Sandbox-Error", cleanupErrorCode(err))
	header := ""
	var owner *processOwnerError
	if errors.As(err, &owner) && owner.PID > 1 && len(owner.State) == 1 && strings.Contains("RSDTtXZPIUW", owner.State) {
		header = fmt.Sprintf(
			"pid=%d;ppid=%d;state=%s;uid=%d;expected_uid=%d",
			owner.PID, owner.ParentPID, owner.State, owner.ObservedUID, owner.ExpectedUID)
	}
	var diagnostic *cleanupDiagnosticError
	if header == "" && errors.As(err, &diagnostic) {
		header = cleanupProcessHeader(diagnostic.snapshots)
	}
	if header != "" {
		w.Header().Set("X-Loom-Sandbox-Process", header)
		// Same bounded fields as the response header, so the sandbox log still
		// has them after the pod is deleted.
		log.Printf("sandbox cleanup diagnostic %s", header)
	}
	http.Error(w, "sandbox process cleanup failed", http.StatusConflict)
}

type runtimeServer struct {
	maxTransfer int64
	maxTimeout  time.Duration
}

type execRequest struct {
	Argv    []string          `json:"argv"`
	Cwd     string            `json:"cwd"`
	Env     map[string]string `json:"env"`
	User    *string           `json:"user"`
	Timeout float64           `json:"timeout_sec"`
}

type execResult struct {
	Code      int     `json:"return_code"`
	Stdout    []byte  `json:"stdout"`
	Stderr    []byte  `json:"stderr"`
	Truncated bool    `json:"truncated"`
	Duration  float64 `json:"duration_sec"`
}

type cappedBuffer struct {
	buffer    bytes.Buffer
	truncated bool
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	remaining := maxOutput - b.buffer.Len()
	if len(p) > remaining {
		p = p[:remaining]
		b.truncated = true
	}
	_, _ = b.buffer.Write(p)
	return n, nil
}

func (s runtimeServer) handler() http.Handler {
	// Identify this server incarnation so the controller cannot reconnect to a
	// restarted sandbox whose writable task state has been lost.
	var identity [16]byte
	if _, err := rand.Read(identity[:]); err != nil {
		panic("cannot initialize sandbox identity")
	}
	instanceID := hex.EncodeToString(identity[:])
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"ready": true, "instance_id": instanceID})
	})
	mux.HandleFunc("POST /exec", s.execute)
	s.registerProcesses(mux, newProcessTable(maxManagedProcesses))
	mux.HandleFunc("PUT /file", s.upload)
	mux.HandleFunc("GET /file", s.download)
	mux.HandleFunc("GET /readlink", s.readlink)
	mux.HandleFunc("POST /restore-directory", s.restoreDirectory)
	var pauseMu sync.Mutex
	var paused pausedProcesses
	mux.HandleFunc("POST /pause-processes", func(w http.ResponseWriter, r *http.Request) {
		pauseMu.Lock()
		defer pauseMu.Unlock()
		if paused != nil {
			http.Error(w, "sandbox already paused", http.StatusConflict)
			return
		}
		var err error
		paused, err = pauseProcesses(r.Context())
		if err != nil {
			paused = nil
			writeCleanupFailure(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /resume-processes", func(w http.ResponseWriter, r *http.Request) {
		pauseMu.Lock()
		defer pauseMu.Unlock()
		if err := resumeProcesses(paused); err != nil {
			writeCleanupFailure(w, err)
			return
		}
		paused = nil
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /stop-processes", func(w http.ResponseWriter, r *http.Request) {
		pauseMu.Lock()
		defer pauseMu.Unlock()
		// Once accepted, finish killing and reaping descendants even if the
		// caller disconnects. stopProcesses retains its own five-second bound.
		if err := stopProcesses(context.WithoutCancel(r.Context())); err != nil {
			writeCleanupFailure(w, err)
			return
		}
		paused = nil
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

// commandRejection is a fixed reason code plus a message without request data.
type commandRejection struct{ code, message string }

func (r *commandRejection) write(w http.ResponseWriter) {
	w.Header().Set("X-Loom-Sandbox-Error", r.code)
	http.Error(w, r.message, http.StatusBadRequest)
}

// validateCommand applies the identity, deadline and environment rules shared
// by buffered /exec and supervised /processes.
func (s runtimeServer) validateCommand(req execRequest) (time.Duration, []string, *commandRejection) {
	if req.User != nil && *req.User != strconv.Itoa(os.Geteuid()) && !(*req.User == "root" && os.Geteuid() == 0) {
		return 0, nil, &commandRejection{"exec_user_mismatch", "exec user must match sandbox UID"}
	}
	if math.IsNaN(req.Timeout) || math.IsInf(req.Timeout, 0) || req.Timeout < 0 || req.Timeout > s.maxTimeout.Seconds() {
		return 0, nil, &commandRejection{"exec_timeout_invalid", "exec timeout outside configured limit"}
	}
	deadline := s.maxTimeout
	if req.Timeout > 0 {
		deadline = time.Duration(req.Timeout * float64(time.Second))
	}
	environment := os.Environ()
	for key, value := range req.Env {
		if key == "" || strings.ContainsAny(key, "=\x00") || strings.ContainsRune(value, '\x00') {
			return 0, nil, &commandRejection{"exec_environment_invalid", "invalid exec environment"}
		}
		environment = append(environment, key+"="+value)
	}
	return deadline, environment, nil
}

func (s runtimeServer) execute(w http.ResponseWriter, r *http.Request) {
	var req execRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024*1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil || len(req.Argv) == 0 {
		w.Header().Set("X-Loom-Sandbox-Error", "exec_request_invalid")
		http.Error(w, "invalid exec request", http.StatusBadRequest)
		return
	}
	deadline, environment, rejection := s.validateCommand(req)
	if rejection != nil {
		rejection.write(w)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), deadline)
	defer cancel()
	cmd := exec.CommandContext(ctx, req.Argv[0], req.Argv[1:]...)
	cmd.Dir = req.Cwd
	cmd.Env = environment
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	cmd.WaitDelay = time.Second
	var stdout, stderr cappedBuffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	started := time.Now()
	err := cmd.Run()
	code := 0
	if err != nil {
		var exitErr *exec.ExitError
		switch {
		case ctx.Err() != nil:
			code = 124
		case errors.Is(err, exec.ErrWaitDelay):
			// A shell exited but left a child holding output pipes open.
			// Bound that execution too; tmux's detached server closes its pipes.
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			code = 124
		case errors.As(err, &exitErr):
			code = exitErr.ExitCode()
		default:
			http.Error(w, "unable to execute sandbox command", http.StatusUnprocessableEntity)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(execResult{code, stdout.buffer.Bytes(), stderr.buffer.Bytes(), stdout.truncated || stderr.truncated, time.Since(started).Seconds()})
}

func (s runtimeServer) upload(w http.ResponseWriter, r *http.Request) {
	mode := uint64(0644)
	if raw := r.Header.Get("X-File-Mode"); raw != "" {
		var err error
		mode, err = strconv.ParseUint(raw, 8, 32)
		if err != nil || mode > 0777 {
			http.Error(w, "invalid file mode", 400)
			return
		}
	}
	if r.ContentLength > s.maxTransfer {
		http.Error(w, "file too large", 413)
		return
	}
	err := writeFile(r.URL.Query().Get("path"), http.MaxBytesReader(w, r.Body, s.maxTransfer), uint32(mode))
	if err != nil {
		var oversized *http.MaxBytesError
		if errors.As(err, &oversized) {
			http.Error(w, "file too large", 413)
			return
		}
		http.Error(w, "file upload rejected", 400)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s runtimeServer) download(w http.ResponseWriter, r *http.Request) {
	f, err := readFile(r.URL.Query().Get("path"))
	if err != nil {
		http.Error(w, "file download rejected", 400)
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		http.Error(w, "not a regular file", 400)
		return
	}
	limit := s.maxTransfer
	if raw := r.URL.Query().Get("max_bytes"); raw != "" {
		requested, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || requested < 0 || requested > limit {
			http.Error(w, "invalid file budget", 400)
			return
		}
		limit = requested
	}
	if info.Size() > limit {
		http.Error(w, "file too large", 413)
		return
	}
	metadata, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		http.Error(w, "file metadata unavailable", 500)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size(), 10))
	w.Header().Set("X-File-Mode", strconv.FormatUint(uint64(info.Mode().Perm()), 8))
	w.Header().Set("X-File-Unix-Mode", strconv.FormatUint(uint64(metadata.Mode), 8))
	w.Header().Set("X-File-UID", strconv.FormatUint(uint64(metadata.Uid), 10))
	w.Header().Set("X-File-GID", strconv.FormatUint(uint64(metadata.Gid), 10))
	_, _ = io.CopyN(w, f, info.Size())
}

func (s runtimeServer) readlink(w http.ResponseWriter, r *http.Request) {
	target, err := readSymlink(r.URL.Query().Get("path"))
	if err != nil {
		http.Error(w, "symlink inspection rejected", 400)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(target)))
	_, _ = io.WriteString(w, target)
}

func checkSocket(path string) error {
	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", path)
	}}}
	resp, err := client.Get("http://sandbox/health")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("sandbox not ready: %d", resp.StatusCode)
	}
	return nil
}

func main() {
	socket := flag.String("socket", "", "Unix socket path in dedicated emptyDir")
	check := flag.String("check-socket", "", "check an existing Unix socket and exit")
	guest := flag.Bool("guest-channel", false, "serve the private guest virtio-serial channel")
	limit := flag.Int64("max-transfer-bytes", 256*1024*1024, "maximum file transfer size")
	timeout := flag.Int("exec-timeout-seconds", 900, "maximum and default exec deadline")
	flag.Parse()
	if *check != "" {
		if err := checkSocket(*check); err != nil {
			log.Fatal("sandbox not ready")
		}
		return
	}
	if (*socket == "" && !*guest) || (*socket != "" && *guest) || *limit <= 0 || *timeout <= 0 {
		log.Fatal("socket and positive limits required")
	}
	var channel io.ReadWriteCloser
	if *guest {
		path, err := guestChannelDevice("/sys/class/virtio-ports", "/dev")
		if err != nil {
			log.Fatal(err)
		}
		device, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			log.Fatalf("guest channel unavailable: %v", err)
		}
		channel = device
	}
	listener, err := sandboxListener(*socket, channel)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	server := &http.Server{Handler: (runtimeServer{*limit, time.Duration(*timeout) * time.Second}).handler(), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second, BaseContext: func(net.Listener) context.Context { return ctx }}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func sandboxListener(socket string, channel io.ReadWriteCloser) (net.Listener, error) {
	if (socket == "") == (channel == nil) {
		return nil, errors.New("exactly one sandbox transport is required")
	}
	if channel != nil {
		return guestchannel.Serve(channel)
	}
	// Never unlink an unknown entry: a new Pod has a fresh private emptyDir.
	listener, err := net.Listen("unix", socket)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(socket, 0660); err != nil {
		listener.Close()
		return nil, err
	}
	return listener, nil
}

func guestChannelDevice(sysRoot, devRoot string) (string, error) {
	entries, err := os.ReadDir(sysRoot)
	if err != nil {
		return "", errors.New("guest serial ports unavailable")
	}
	pattern := regexp.MustCompile(`^vport[0-9]+p[0-9]+$`)
	result := ""
	for _, entry := range entries {
		if !pattern.MatchString(entry.Name()) {
			continue
		}
		name, err := os.ReadFile(filepath.Join(sysRoot, entry.Name(), "name"))
		if err != nil {
			return "", errors.New("cannot inspect guest serial port")
		}
		if strings.TrimSpace(string(name)) != "loom.rpc" {
			continue
		}
		if result != "" {
			return "", errors.New("ambiguous guest RPC port")
		}
		result = filepath.Join(devRoot, entry.Name())
	}
	if result == "" {
		return "", errors.New("guest RPC port missing")
	}
	return result, nil
}
