package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	nativeTrajectoryMaxEventBytes  = 256 * 1024
	nativeTrajectoryMaxQueueBytes  = 4 * 1024 * 1024
	nativeTrajectoryMaxQueueEvents = 256
	nativeTrajectoryMaxBatchBytes  = 1024 * 1024
	nativeTrajectoryMaxReadBytes   = 1024 * 1024
	nativeTrajectoryMaxFailures    = 3
	nativeTrajectoryMaxTotalBytes  = 16 * 1024 * 1024
	nativeTrajectoryMaxTotalEvents = 10000
)

type nativeTrajectoryRequest struct {
	workloadIdentity
	SchemaVersion string            `json:"schema_version"`
	Events        []json.RawMessage `json:"events"`
}
type nativeTrajectoryOptions struct{ pollInterval, requestTimeout, stopTimeout time.Duration }

func defaultNativeTrajectoryOptions() nativeTrajectoryOptions {
	return nativeTrajectoryOptions{time.Second, 2 * time.Second, 3 * time.Second}
}

type nativeTrajectoryBoundary struct {
	role     string
	at       time.Time
	starting bool
}
type nativeTrajectoryFile struct {
	source   string
	offset   int64
	pending  []byte
	identity os.FileInfo
	disabled bool
}

// This preview has independent sequence numbers and durability from the canonical
// output commit. Losing preview delivery must never fail or delay task execution.
// Only the trusted Go controller owns the Pod credential and HTTP operation.
type nativeTrajectory struct {
	broker        *workloadBroker
	workspace     string
	options       nativeTrajectoryOptions
	ctx           context.Context
	cancel        context.CancelFunc
	boundaries    chan nativeTrajectoryBoundary
	stop          chan struct{}
	done          chan struct{}
	stopOnce      sync.Once
	overflow      atomic.Bool
	diagnostic    sync.Once
	files         []nativeTrajectoryFile
	trialID       string
	startedAt     time.Time
	sequence      int64
	queue         []json.RawMessage
	queueBytes    int
	totalBytes    int
	totalLimit    int
	readBytes     int
	inflightCount int
	failures      int
	disabled      bool
}

func startNativeTrajectory(parent context.Context, broker *workloadBroker, p plan, workspace string, override ...nativeTrajectoryOptions) *nativeTrajectory {
	if broker == nil || broker.identity.ExecutionRole != "attempt" || p.ExecutionRole != "attempt" {
		return nil
	}
	options := defaultNativeTrajectoryOptions()
	if len(override) > 0 {
		options = override[0]
	}
	limit := min(p.MaxArtifactBytes, nativeTrajectoryMaxTotalBytes)
	if limit <= 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(parent)
	u := &nativeTrajectory{broker: broker, workspace: filepath.Clean(workspace), options: options, ctx: ctx, cancel: cancel, startedAt: time.Now().UTC(), totalLimit: int(limit), boundaries: make(chan nativeTrajectoryBoundary, 64), stop: make(chan struct{}), done: make(chan struct{})}
	// The immutable plan, never a task path/header, selects the typed trajectory.
	seen := map[string]bool{}
	for _, d := range p.OutputDeclarations {
		source := filepath.FromSlash(d.SourcePath)
		if d.Kind != "trajectory" || seen[source] {
			continue
		}
		if source == "." || filepath.IsAbs(source) || filepath.Clean(source) != source || !isWithin(u.workspace, filepath.Join(u.workspace, source)) || len(u.files) >= 8 {
			u.warn("invalid_declared_path")
			continue
		}
		seen[source] = true
		u.files = append(u.files, nativeTrajectoryFile{source: source})
	}
	go u.run()
	return u
}

func (u *nativeTrajectory) warn(reason string) {
	u.diagnostic.Do(func() { fmt.Fprintln(os.Stderr, "live trajectory preview unavailable:", reason) })
}

// runPhase calls this at real phase entry/exit. Queueing is nonblocking and
// never performs file I/O, networking, credential reads, or retries.
func (u *nativeTrajectory) phaseBoundary(role string, deadline time.Time) {
	if u == nil {
		return
	}
	switch role {
	case "setup", "agent", "verifier":
	default:
		return
	}
	select {
	case u.boundaries <- nativeTrajectoryBoundary{role: role, at: time.Now().UTC(), starting: !deadline.IsZero()}:
	default:
		u.overflow.Store(true)
	}
}

func (u *nativeTrajectory) close() {
	if u == nil {
		return
	}
	u.stopOnce.Do(func() { close(u.stop) })
	timer := time.NewTimer(u.options.stopTimeout)
	defer timer.Stop()
	select {
	case <-u.done:
	case <-timer.C:
		u.cancel()
	}
}

func (u *nativeTrajectory) binding() (map[string]string, error) {
	ctx, cancel := context.WithTimeout(u.ctx, u.options.requestTimeout)
	defer cancel()
	response, err := u.broker.getInputOnce(ctx, u.broker.endpoint("/llm-calls"))
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 512*1024+1))
	if err != nil || len(body) > 512*1024 {
		return nil, fmt.Errorf("identity response unavailable")
	}
	var data struct {
		TrialID   string `json:"trial_id"`
		StepID    string `json:"step_id"`
		TaskID    string `json:"task_id"`
		AgentName string `json:"agent_name"`
	}
	if json.Unmarshal(body, &data) != nil || !materializationID.MatchString(data.TrialID) || data.StepID != "agent" {
		return nil, fmt.Errorf("identity response invalid")
	}
	return map[string]string{"trial_id": data.TrialID, "task_id": data.TaskID, "agent_name": data.AgentName}, nil
}

func (u *nativeTrajectory) run() {
	defer close(u.done)
	defer u.cancel()
	var binding map[string]string
	for n := 0; n < nativeTrajectoryMaxFailures; n++ {
		var err error
		binding, err = u.binding()
		if err == nil {
			break
		}
		if n == nativeTrajectoryMaxFailures-1 {
			u.warn("identity_lookup_failed")
			return
		}
		select {
		case <-u.ctx.Done():
			return
		case <-u.stop:
			return
		case <-time.After(u.options.pollInterval):
		}
	}
	u.trialID = binding["trial_id"]
	if binding["task_id"] != "" && binding["agent_name"] != "" {
		u.enqueue(map[string]any{"kind": "trial_start", "trial_id": u.trialID, "step_id": "trial", "emitted_at": u.startedAt, "task_id": binding["task_id"], "agent_name": binding["agent_name"], "agent_mode": "out-of-box"})
	}
	ticker := time.NewTicker(u.options.pollInterval)
	defer ticker.Stop()
	for {
		if u.disabled || u.overflow.Load() {
			u.warn("preview_bound_exceeded")
			return
		}
		select {
		case <-u.ctx.Done():
			return
		case <-u.stop:
			for {
				select {
				case boundary := <-u.boundaries:
					u.observeBoundary(boundary)
				default:
					u.tail()
					for len(u.queue) > 0 && !u.disabled && u.ctx.Err() == nil {
						u.flush()
					}
					return
				}
			}
		case boundary := <-u.boundaries:
			u.observeBoundary(boundary)
			u.flush()
		case <-ticker.C:
			u.tail()
			u.flush()
		}
	}
}

func (u *nativeTrajectory) observeBoundary(boundary nativeTrajectoryBoundary) {
	if !boundary.starting {
		u.tail()
	}
	event := map[string]any{"trial_id": u.trialID, "step_id": boundary.role, "emitted_at": boundary.at}
	if boundary.starting {
		event["kind"] = "step_start"
		event["instruction_excerpt"] = "Native execution phase started"
	} else {
		event["kind"] = "step_end"
	}
	u.enqueue(event)
}

func (u *nativeTrajectory) enqueue(event map[string]any) {
	if u.disabled {
		return
	}
	event["seq"] = u.sequence
	payload, err := json.Marshal(event)
	if err != nil || len(payload) > nativeTrajectoryMaxEventBytes {
		u.warn("event_size_invalid")
		return
	}
	if len(u.queue) >= nativeTrajectoryMaxQueueEvents || u.queueBytes+len(payload) > nativeTrajectoryMaxQueueBytes || u.totalBytes+len(payload) > u.totalLimit || u.sequence >= nativeTrajectoryMaxTotalEvents {
		u.disabled = true
		u.warn("preview_bound_exceeded")
		return
	}
	u.queue = append(u.queue, payload)
	u.queueBytes += len(payload)
	u.totalBytes += len(payload)
	u.sequence++
}

func (u *nativeTrajectory) tail() {
	remaining := min(nativeTrajectoryMaxReadBytes, u.totalLimit-u.readBytes)
	if remaining <= 0 {
		u.disabled = true
		u.warn("preview_bound_exceeded")
		return
	}
	for n := range u.files {
		f := &u.files[n]
		if f.disabled || remaining <= 0 || u.disabled {
			continue
		}
		file, info, err := openNativeTrajectoryFile(u.workspace, f.source)
		if err != nil {
			if !os.IsNotExist(err) {
				f.disabled = true
				u.warn("trajectory_path_unavailable")
			}
			continue
		}
		if (f.identity != nil && !os.SameFile(f.identity, info)) || info.Size() < f.offset {
			file.Close()
			f.disabled = true
			u.warn("trajectory_file_replaced")
			continue
		}
		f.identity = info
		chunk := make([]byte, min(64*1024, remaining))
		for remaining > 0 && !f.disabled && !u.disabled {
			size, readErr := file.ReadAt(chunk[:min(len(chunk), remaining)], f.offset)
			if size > 0 {
				f.offset += int64(size)
				remaining -= size
				u.readBytes += size
				f.pending = append(f.pending, chunk[:size]...)
				for {
					newline := bytes.IndexByte(f.pending, '\n')
					if newline < 0 {
						break
					}
					if newline > nativeTrajectoryMaxEventBytes {
						f.disabled = true
						u.warn("event_size_invalid")
						break
					}
					line := bytes.TrimSpace(f.pending[:newline])
					f.pending = f.pending[newline+1:]
					if len(line) == 0 {
						continue
					}
					var event map[string]any
					var envelope struct {
						TrialID   string    `json:"trial_id"`
						StepID    string    `json:"step_id"`
						Kind      string    `json:"kind"`
						Seq       *int64    `json:"seq"`
						EmittedAt time.Time `json:"emitted_at"`
					}
					if json.Unmarshal(line, &envelope) != nil || envelope.TrialID != u.trialID || envelope.Kind == "" || envelope.StepID == "" || envelope.Seq == nil || *envelope.Seq < 0 || envelope.EmittedAt.IsZero() || decodeNativeTrajectoryEvent(line, &event) != nil {
						f.disabled = true
						u.warn("trajectory_event_invalid")
						break
					}
					u.enqueue(event)
				}
				if len(f.pending) > nativeTrajectoryMaxEventBytes {
					f.disabled = true
					u.warn("event_size_invalid")
				}
			}
			if readErr != nil {
				if readErr != io.EOF {
					f.disabled = true
					u.warn("trajectory_read_failed")
				}
				break
			}
			if size == 0 {
				break
			}
		}
		file.Close()
		if f.disabled {
			f.pending = nil
		}
	}
}

func (u *nativeTrajectory) flush() {
	if len(u.queue) == 0 || u.disabled {
		return
	}
	count := u.inflightCount
	if count == 0 {
		size := 0
		for _, event := range u.queue {
			if count >= 50 || size+len(event) > nativeTrajectoryMaxBatchBytes {
				break
			}
			size += len(event)
			count++
		}
		if count == 0 {
			return
		}
		u.inflightCount = count
	}
	size := 0
	for _, event := range u.queue[:count] {
		size += len(event)
	}
	ctx, cancel := context.WithTimeout(u.ctx, u.options.requestTimeout)
	defer cancel()
	request := nativeTrajectoryRequest{workloadIdentity: u.broker.identity, SchemaVersion: "loom.service-execution-trajectory-events.v1", Events: u.queue[:count]}
	var response struct {
		NextAfterSeq int64 `json:"next_after_seq"`
	}
	err := u.broker.doJSON(ctx, http.MethodPost, u.broker.endpoint("/trajectory-events"), request, nil, &response)
	var last struct {
		Seq int64 `json:"seq"`
	}
	_ = json.Unmarshal(u.queue[count-1], &last)
	if err != nil || response.NextAfterSeq != last.Seq {
		u.failures++
		if u.failures >= nativeTrajectoryMaxFailures {
			u.disabled = true
			u.warn("event_delivery_failed")
		}
		return
	}
	u.failures = 0
	u.queue = u.queue[count:]
	u.queueBytes -= size
	u.inflightCount = 0
}

// A trajectory declaration is the only accepted source. Component validation is
// repeated by the descriptor-anchored Linux open helper; task-owned symlinks may
// never redirect this controller into a token file, log, or another workspace.
func nativeTrajectoryComponents(source string) ([]string, error) {
	if source == "" || filepath.IsAbs(source) || filepath.Clean(source) != source {
		return nil, fmt.Errorf("invalid trajectory path")
	}
	components := strings.Split(source, string(filepath.Separator))
	for _, component := range components {
		if component == "" || component == "." || component == ".." {
			return nil, fmt.Errorf("invalid trajectory path")
		}
	}
	return components, nil
}

func decodeNativeTrajectoryEvent(payload []byte, event *map[string]any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	return decoder.Decode(event)
}
