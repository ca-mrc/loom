package main

// Supervised long-running processes for installed agent harnesses (#2310).
//
// Unlike buffered /exec, a process started here outlives the request. The
// controller reads stdout/stderr by offset, waits for its exit status and can
// kill its process group. Output is retained in a bounded window per stream:
// a slow reader pauses the process through pipe backpressure instead of losing
// output, and bytes are released once the reader moves past them.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const (
	maxManagedProcesses    = 16
	processStreamWindow    = 4 * 1024 * 1024
	maxProcessReadBytes    = 1024 * 1024
	maxProcessPollDuration = 10 * time.Second
	// Time for output pipes to drain after the process exits before any
	// descendant still holding them is killed, matching /exec's WaitDelay.
	processPipeDrainGrace = time.Second
)

var errProcessOffset = errors.New("process output offset is not available")

// processStream is one output stream's bounded, offset-addressed window.
type processStream struct {
	mu      sync.Mutex
	data    []byte
	base    int64 // offset of data[0]
	closed  bool  // the pipe reached EOF; no more data will arrive
	aborted bool  // the process was stopped; further output is discarded
	dropped bool  // output was discarded after abort
	limit   int
	changed chan struct{}
}

func newProcessStream(limit int) *processStream {
	return &processStream{limit: limit, changed: make(chan struct{})}
}

func (s *processStream) notifyLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

// write blocks while the unread window is full, so the writing pipe and then
// the process itself pause until the controller reads.
func (s *processStream) write(p []byte) {
	for len(p) > 0 {
		s.mu.Lock()
		for len(s.data) >= s.limit && !s.aborted {
			changed := s.changed
			s.mu.Unlock()
			<-changed
			s.mu.Lock()
		}
		if s.aborted {
			s.dropped = true
			s.mu.Unlock()
			return
		}
		n := min(s.limit-len(s.data), len(p))
		s.data = append(s.data, p[:n]...)
		p = p[n:]
		s.notifyLocked()
		s.mu.Unlock()
	}
}

func (s *processStream) finish() {
	s.mu.Lock()
	s.closed = true
	s.notifyLocked()
	s.mu.Unlock()
}

func (s *processStream) abort() {
	s.mu.Lock()
	s.aborted = true
	s.notifyLocked()
	s.mu.Unlock()
}

// read returns bytes at offset, waiting up to wait for new data. Reading from
// offset releases everything before it. eof means no bytes exist past next.
func (s *processStream) read(ctx context.Context, offset int64, limit int, wait time.Duration) (chunk []byte, next int64, eof, dropped bool, err error) {
	timer := time.NewTimer(wait)
	defer timer.Stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		if offset < s.base || offset > s.base+int64(len(s.data)) {
			return nil, offset, false, s.dropped, errProcessOffset
		}
		if consumed := int(offset - s.base); consumed > 0 {
			s.data = append([]byte(nil), s.data[consumed:]...)
			s.base = offset
			s.notifyLocked()
		}
		if len(s.data) > 0 || s.closed {
			n := min(len(s.data), limit)
			chunk = append([]byte(nil), s.data[:n]...)
			return chunk, offset + int64(n), s.closed && n == len(s.data), s.dropped, nil
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-timer.C:
			s.mu.Lock()
			return nil, offset, false, s.dropped, nil
		case <-ctx.Done():
			s.mu.Lock()
			return nil, offset, false, s.dropped, ctx.Err()
		}
		s.mu.Lock()
	}
}

type processStatus struct {
	ID          string  `json:"id"`
	PID         int     `json:"pid"`
	Running     bool    `json:"running"`
	ExitCode    *int    `json:"exit_code"`
	TimedOut    bool    `json:"timed_out"`
	Killed      bool    `json:"killed"`
	DurationSec float64 `json:"duration_sec"`
}

type managedProcess struct {
	id      string
	cmd     *exec.Cmd
	cancel  context.CancelFunc
	stdout  *processStream
	stderr  *processStream
	started time.Time
	exited  chan struct{} // closed once the exit status is recorded

	mu       sync.Mutex
	code     int
	timedOut bool
	killed   bool
	finished time.Time
}

func (p *managedProcess) killGroup() {
	_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
}

func (p *managedProcess) status() processStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	status := processStatus{ID: p.id, PID: p.cmd.Process.Pid, TimedOut: p.timedOut, Killed: p.killed}
	select {
	case <-p.exited:
		code := p.code
		status.ExitCode = &code
		status.DurationSec = p.finished.Sub(p.started).Seconds()
	default:
		status.Running = true
		status.DurationSec = time.Since(p.started).Seconds()
	}
	return status
}

type processTable struct {
	mu    sync.Mutex
	items map[string]*managedProcess
	max   int
}

func newProcessTable(max int) *processTable {
	return &processTable{items: map[string]*managedProcess{}, max: max}
}

func (t *processTable) get(id string) *managedProcess {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.items[id]
}

func pumpOutput(source *os.File, stream *processStream, done *sync.WaitGroup) {
	defer done.Done()
	defer stream.finish()
	buffer := make([]byte, 32*1024)
	for {
		n, err := source.Read(buffer)
		if n > 0 {
			stream.write(buffer[:n])
		}
		if err != nil {
			return
		}
	}
}

func (s runtimeServer) registerProcesses(mux *http.ServeMux, table *processTable) {
	mux.HandleFunc("POST /processes", func(w http.ResponseWriter, r *http.Request) {
		s.startProcess(w, r, table)
	})
	mux.HandleFunc("GET /processes/{id}", func(w http.ResponseWriter, r *http.Request) {
		process := table.get(r.PathValue("id"))
		if process == nil {
			http.Error(w, "unknown process", http.StatusNotFound)
			return
		}
		if wait, ok := pollDuration(w, r); !ok {
			return
		} else if wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-process.exited:
			case <-timer.C:
			case <-r.Context().Done():
			}
			timer.Stop()
		}
		writeJSON(w, process.status())
	})
	mux.HandleFunc("GET /processes/{id}/output", func(w http.ResponseWriter, r *http.Request) {
		process := table.get(r.PathValue("id"))
		if process == nil {
			http.Error(w, "unknown process", http.StatusNotFound)
			return
		}
		var stream *processStream
		switch r.URL.Query().Get("stream") {
		case "stdout":
			stream = process.stdout
		case "stderr":
			stream = process.stderr
		default:
			http.Error(w, "stream must be stdout or stderr", http.StatusBadRequest)
			return
		}
		offset, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
		if err != nil || offset < 0 {
			http.Error(w, "invalid offset", http.StatusBadRequest)
			return
		}
		wait, ok := pollDuration(w, r)
		if !ok {
			return
		}
		chunk, next, eof, dropped, err := stream.read(r.Context(), offset, maxProcessReadBytes, wait)
		if errors.Is(err, errProcessOffset) {
			w.Header().Set("X-Loom-Sandbox-Error", "process_offset_unavailable")
			http.Error(w, "output offset no longer available", http.StatusConflict)
			return
		}
		if err != nil {
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("X-Loom-Next-Offset", strconv.FormatInt(next, 10))
		if eof {
			w.Header().Set("X-Loom-EOF", "1")
		}
		if dropped {
			w.Header().Set("X-Loom-Output-Dropped", "1")
		}
		_, _ = w.Write(chunk)
	})
	mux.HandleFunc("POST /processes/{id}/kill", func(w http.ResponseWriter, r *http.Request) {
		process := table.get(r.PathValue("id"))
		if process == nil {
			http.Error(w, "unknown process", http.StatusNotFound)
			return
		}
		select {
		case <-process.exited:
		default:
			process.mu.Lock()
			process.killed = true
			process.mu.Unlock()
			process.killGroup()
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("DELETE /processes/{id}", func(w http.ResponseWriter, r *http.Request) {
		table.mu.Lock()
		defer table.mu.Unlock()
		process := table.items[r.PathValue("id")]
		if process == nil {
			http.Error(w, "unknown process", http.StatusNotFound)
			return
		}
		select {
		case <-process.exited:
		default:
			http.Error(w, "process is still running", http.StatusConflict)
			return
		}
		delete(table.items, process.id)
		w.WriteHeader(http.StatusNoContent)
	})
}

func pollDuration(w http.ResponseWriter, r *http.Request) (time.Duration, bool) {
	raw := r.URL.Query().Get("wait_ms")
	if raw == "" {
		return 0, true
	}
	ms, err := strconv.Atoi(raw)
	if err != nil || ms < 0 || time.Duration(ms)*time.Millisecond > maxProcessPollDuration {
		http.Error(w, "invalid wait", http.StatusBadRequest)
		return 0, false
	}
	return time.Duration(ms) * time.Millisecond, true
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func (s runtimeServer) startProcess(w http.ResponseWriter, r *http.Request, table *processTable) {
	var req execRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024*1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil || len(req.Argv) == 0 {
		w.Header().Set("X-Loom-Sandbox-Error", "exec_request_invalid")
		http.Error(w, "invalid process request", http.StatusBadRequest)
		return
	}
	deadline, environment, rejection := s.validateCommand(req)
	if rejection != nil {
		rejection.write(w)
		return
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	if len(table.items) >= table.max {
		w.Header().Set("X-Loom-Sandbox-Error", "process_limit_reached")
		http.Error(w, "too many supervised processes", http.StatusTooManyRequests)
		return
	}
	var identity [12]byte
	if _, err := rand.Read(identity[:]); err != nil {
		http.Error(w, "unable to start sandbox process", http.StatusInternalServerError)
		return
	}
	// The process outlives this request; only its own deadline and an
	// explicit kill end it.
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
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
	stdoutRead, stdoutWrite, err := os.Pipe()
	if err != nil {
		cancel()
		http.Error(w, "unable to start sandbox process", http.StatusInternalServerError)
		return
	}
	stderrRead, stderrWrite, err := os.Pipe()
	if err != nil {
		cancel()
		_ = stdoutRead.Close()
		_ = stdoutWrite.Close()
		http.Error(w, "unable to start sandbox process", http.StatusInternalServerError)
		return
	}
	// Files, not writers: Wait then never waits on output copying, so exit
	// status is known even while a descendant still holds the pipes.
	cmd.Stdout, cmd.Stderr = stdoutWrite, stderrWrite
	startErr := cmd.Start()
	_ = stdoutWrite.Close()
	_ = stderrWrite.Close()
	if startErr != nil {
		cancel()
		_ = stdoutRead.Close()
		_ = stderrRead.Close()
		http.Error(w, "unable to execute sandbox command", http.StatusUnprocessableEntity)
		return
	}
	process := &managedProcess{
		id: hex.EncodeToString(identity[:]), cmd: cmd, cancel: cancel,
		stdout: newProcessStream(processStreamWindow), stderr: newProcessStream(processStreamWindow),
		started: time.Now(), exited: make(chan struct{}),
	}
	var pumps sync.WaitGroup
	pumps.Add(2)
	go pumpOutput(stdoutRead, process.stdout, &pumps)
	go pumpOutput(stderrRead, process.stderr, &pumps)
	go superviseProcess(ctx, process, &pumps, stdoutRead, stderrRead)
	table.items[process.id] = process
	w.WriteHeader(http.StatusCreated)
	writeJSON(w, process.status())
}

func superviseProcess(ctx context.Context, process *managedProcess, pumps *sync.WaitGroup, pipes ...io.Closer) {
	err := process.cmd.Wait()
	code := 0
	var exitErr *exec.ExitError
	switch {
	case ctx.Err() == context.DeadlineExceeded:
		code = 124
	case errors.As(err, &exitErr):
		code = exitErr.ExitCode()
		if status, ok := exitErr.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			code = 128 + int(status.Signal())
		}
	case err != nil:
		code = 1
	}
	process.mu.Lock()
	process.code = code
	process.timedOut = ctx.Err() == context.DeadlineExceeded
	process.finished = time.Now()
	process.mu.Unlock()
	close(process.exited)
	process.cancel()
	drained := make(chan struct{})
	go func() { pumps.Wait(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(processPipeDrainGrace):
		// A descendant kept the pipes open after the process exited. Bound it
		// like /exec does, then stop accepting its output.
		process.killGroup()
		process.stdout.abort()
		process.stderr.abort()
		for _, pipe := range pipes {
			_ = pipe.Close()
		}
		<-drained
	}
	for _, pipe := range pipes {
		_ = pipe.Close()
	}
}
