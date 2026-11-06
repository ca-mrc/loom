package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type streamEvidence struct {
	Path       string `json:"path"`
	SHA256     string `json:"sha256"`
	BytesSeen  int64  `json:"bytes_seen"`
	BytesSaved int64  `json:"bytes_saved"`
	Truncated  bool   `json:"truncated"`
}

type phaseEvidence struct {
	verifierReportPrepared bool
	Role                   string         `json:"role"`
	Ordinal                int            `json:"ordinal"`
	StartedAt              time.Time      `json:"started_at"`
	FinishedAt             time.Time      `json:"finished_at"`
	ExitCode               int            `json:"exit_code"`
	Signal                 string         `json:"signal,omitempty"`
	TimedOut               bool           `json:"timed_out"`
	Stdout                 streamEvidence `json:"stdout"`
	Stderr                 streamEvidence `json:"stderr"`
}

type outputEvidence struct {
	SourcePath   string `json:"source_path"`
	RelativePath string `json:"relative_path"`
	Kind         string `json:"kind"`
	Required     bool   `json:"required"`
	State        string `json:"state"`
	SizeBytes    *int64 `json:"size_bytes"`
	SHA256       string `json:"sha256,omitempty"`
}

type resultManifest struct {
	verifierReportPrepared bool
	SchemaVersion          string             `json:"schema_version"`
	RuntimeContractSHA256  string             `json:"runtime_contract_sha256"`
	CandidateSHA           string             `json:"candidate_sha"`
	TaskRevisionSHA256     string             `json:"task_revision_sha256"`
	CommandIdentitySHA256  string             `json:"command_identity_sha256"`
	ExecutionRole          string             `json:"execution_role"`
	ContainerRoles         []string           `json:"container_roles"`
	TaskImageRef           string             `json:"task_image_ref"`
	RuntimeImageRef        string             `json:"runtime_image_ref"`
	RuntimeBinarySHA256    string             `json:"runtime_binary_sha256"`
	ExecutionClassID       string             `json:"execution_class_id"`
	Status                 string             `json:"status"`
	FailureReason          string             `json:"failure_reason,omitempty"`
	StartedAt              time.Time          `json:"started_at"`
	FinishedAt             time.Time          `json:"finished_at"`
	Phases                 []phaseEvidence    `json:"phases"`
	Outputs                []outputEvidence   `json:"outputs"`
	VerifierRewards        map[string]float64 `json:"verifier_rewards,omitempty"`
	PartialEvidence        bool               `json:"partial_evidence"`
}

type terminationSummary struct {
	SchemaVersion         string    `json:"schema_version"`
	RuntimeContractSHA256 string    `json:"runtime_contract_sha256"`
	CommandIdentitySHA256 string    `json:"command_identity_sha256"`
	ExecutionRole         string    `json:"execution_role"`
	Status                string    `json:"status"`
	PartialEvidence       bool      `json:"partial_evidence"`
	PhaseCount            int       `json:"phase_count"`
	FinishedAt            time.Time `json:"finished_at"`
	ResultPath            string    `json:"result_path"`
	OutputCommitted       bool      `json:"output_committed"`
	OutputUploadSessionID string    `json:"output_upload_session_id,omitempty"`
	OutputManifestSHA256  string    `json:"output_manifest_sha256,omitempty"`
	OutputMarkerSHA256    string    `json:"output_marker_sha256,omitempty"`
}

type boundedWriter struct {
	file    *os.File
	hash    hashWriter
	console io.Writer
	limit   int64
	seen    int64
	saved   int64
}

type hashWriter interface {
	Write([]byte) (int, error)
	Sum([]byte) []byte
}

func (w *boundedWriter) Write(payload []byte) (int, error) {
	w.seen += int64(len(payload))
	_, _ = w.hash.Write(payload)
	remaining := w.limit - w.saved
	if remaining > 0 {
		chunk := payload
		if int64(len(chunk)) > remaining {
			chunk = chunk[:remaining]
		}
		if _, err := w.file.Write(chunk); err != nil {
			return 0, err
		}
		if _, err := w.console.Write(chunk); err != nil {
			return 0, err
		}
		w.saved += int64(len(chunk))
	}
	return len(payload), nil
}

func (w *boundedWriter) evidence(path string) streamEvidence {
	return streamEvidence{
		Path: path, SHA256: "sha256:" + hex.EncodeToString(w.hash.Sum(nil)),
		BytesSeen: w.seen, BytesSaved: w.saved, Truncated: w.seen > w.saved,
	}
}

func runPlan(
	ctx context.Context,
	p plan,
	workspace, outputRoot string,
	trustedEnvironment map[string]string,
	phaseBoundary ...func(string, time.Time),
) (resultManifest, error) {
	started := time.Now().UTC()
	result := resultManifest{
		SchemaVersion: "loom.execution-runtime-result.v1", RuntimeContractSHA256: p.RuntimeContractSHA256,
		CandidateSHA: p.CandidateSHA, CommandIdentitySHA256: p.CommandIdentitySHA256,
		ExecutionRole:      p.ExecutionRole,
		TaskRevisionSHA256: p.TaskRevisionSHA256, TaskImageRef: p.TaskImageRef,
		RuntimeImageRef: p.RuntimeImageRef, RuntimeBinarySHA256: p.RuntimeBinarySHA256,
		ExecutionClassID: p.ExecutionClassID, Status: "running", StartedAt: started,
		Phases: []phaseEvidence{},
	}
	result.ContainerRoles = []string{"execution", p.Main.Role}
	for _, item := range p.Sidecars {
		result.ContainerRoles = append(result.ContainerRoles, item.RoleName)
	}
	if p.Verifier != nil {
		result.ContainerRoles = append(result.ContainerRoles, "verifier")
	}
	if err := secureDirectory(workspace); err != nil {
		result.Status = "runtime_error"
		result.PartialEvidence = true
		result.FinishedAt = time.Now().UTC()
		return result, err
	}
	if err := secureDirectory(outputRoot); err != nil {
		result.Status = "runtime_error"
		result.PartialEvidence = true
		result.FinishedAt = time.Now().UTC()
		return result, err
	}
	phases := append([]phase{}, p.Setup...)
	phases = append(phases, p.Main)
	if p.Verifier != nil {
		phases = append(phases, *p.Verifier)
	}
	var agentTimeout error
	for ordinal, item := range phases {
		if ctx.Err() != nil {
			result.Status = classifyFailure(ctx, phaseEvidence{})
			result.PartialEvidence = true
			result.FinishedAt = time.Now().UTC()
			if errors.Is(context.Cause(ctx), errSandboxLost) {
				result.FailureReason = "sandbox_lost"
			}
			return result, context.Cause(ctx)
		}
		transportMarkerReady := false
		if item.Role == "agent" {
			// Input bundles and prior phases cannot supply classification for
			// this invocation. Only a marker written during the phase counts.
			root, openErr := os.OpenRoot(workspace)
			if openErr == nil {
				openErr = root.Remove(".loom/agent/transport-failure.json")
				root.Close()
			}
			transportMarkerReady = openErr == nil || os.IsNotExist(openErr)
		}
		evidence, err := runPhase(
			ctx, item, ordinal+1, workspace, outputRoot,
			p.MaxLogBytesPerStream, time.Duration(p.TerminationGraceSec)*time.Second,
			trustedEnvironment, phaseBoundary...,
		)
		result.Phases = append(result.Phases, evidence)
		if evidence.verifierReportPrepared {
			result.verifierReportPrepared = true
		}
		if errors.Is(context.Cause(ctx), errSandboxLost) {
			err = context.Cause(ctx)
		}
		if err != nil {
			// Exit 124 is the trusted controller's acknowledgement that an
			// expired agent is quiescent and its workspace handoff is complete.
			// A deadline alone (or a forced kill) is never a safe handoff.
			if p.VerifierAfterAgentTimeout && item.Role == "agent" &&
				evidence.TimedOut && evidence.ExitCode == 124 && ctx.Err() == nil {
				agentTimeout = err
				result.PartialEvidence = true
				continue
			}
			result.Status = classifyFailure(ctx, evidence)
			if result.Status == "task_error" && sandboxCleanupFailure(workspace, evidence.Role) {
				result.Status = "runtime_error"
			}
			if errors.Is(context.Cause(ctx), errSandboxLost) {
				result.FailureReason = "sandbox_lost"
				err = context.Cause(ctx)
			}
			if agentTimeout != nil && ctx.Err() == nil {
				result.Status = "timed_out"
				err = errors.Join(agentTimeout, err)
			}
			if result.Status == "task_error" && p.ExecutionRole == "attempt" && evidence.Role == "agent" && transportMarkerReady {
				result.FailureReason = agentTransportFailure(workspace)
			}
			result.PartialEvidence = true
			result.FinishedAt = time.Now().UTC()
			return result, err
		}
	}
	if agentTimeout != nil {
		result.Status = "timed_out"
		result.FinishedAt = time.Now().UTC()
		return result, agentTimeout
	}
	result.Status = "succeeded"
	result.FinishedAt = time.Now().UTC()
	return result, nil
}

func openPhaseContext(
	parent context.Context, timeout time.Duration, updates <-chan time.Time,
) (context.Context, context.CancelFunc, time.Time, func() time.Time) {
	deadline := time.Now().Add(timeout)
	if updates == nil {
		ctx, cancel := context.WithTimeout(parent, timeout)
		if actual, ok := ctx.Deadline(); ok {
			deadline = actual
		}
		return ctx, cancel, deadline, func() time.Time { return deadline }
	}
	ctx, cancel := context.WithCancelCause(parent)
	var mu sync.Mutex
	current := deadline
	timer := time.NewTimer(time.Until(current))
	go func() {
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				cancel(context.DeadlineExceeded)
				return
			case next, ok := <-updates:
				if !ok {
					return
				}
				mu.Lock()
				if !next.After(current) {
					mu.Unlock()
					continue
				}
				current = next
				remaining := time.Until(current)
				mu.Unlock()
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(remaining)
			}
		}
	}()
	return ctx, func() { cancel(context.Canceled) }, deadline, func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return current
	}
}

func runPhase(
	parent context.Context,
	item phase,
	ordinal int,
	workspace, outputRoot string,
	limit int64,
	terminationGrace time.Duration,
	trustedEnvironment map[string]string,
	phaseBoundary ...func(string, time.Time),
) (phaseEvidence, error) {
	phaseCtx, cancel, deadline, currentDeadline := openPhaseContext(
		parent, time.Duration(item.TimeoutSeconds)*time.Second, item.DeadlineUpdates,
	)
	defer cancel()
	for _, boundary := range phaseBoundary {
		boundary(item.Role, deadline)
		defer boundary(item.Role, time.Time{})
	}
	directory := filepath.Clean(item.WorkingDirectory)
	// /app is the fixed trusted controller directory. Do not normalize task
	// paths into that exception or allow arbitrary sibling application paths.
	if directory != item.WorkingDirectory ||
		(directory != "/app" && directory != workspace && !isWithin(workspace, directory)) {
		return phaseEvidence{}, fmt.Errorf("phase working directory escapes workspace or trusted /app")
	}
	prefix := fmt.Sprintf("%02d-%s", ordinal, item.Role)
	stdoutPath := filepath.Join(outputRoot, prefix+".stdout")
	stderrPath := filepath.Join(outputRoot, prefix+".stderr")
	stdoutFile, err := os.OpenFile(stdoutPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return phaseEvidence{}, err
	}
	defer stdoutFile.Close()
	stderrFile, err := os.OpenFile(stderrPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return phaseEvidence{}, err
	}
	defer stderrFile.Close()
	stdout := &boundedWriter{file: stdoutFile, hash: sha256.New(), console: os.Stdout, limit: limit}
	stderr := &boundedWriter{file: stderrFile, hash: sha256.New(), console: os.Stderr, limit: limit}
	command := exec.Command(item.Argv[0], item.Argv[1:]...)
	command.Dir = directory
	environment := map[string]string{}
	for _, entry := range os.Environ() {
		name, value, found := strings.Cut(entry, "=")
		if found && !strings.HasPrefix(name, "LOOM_EXECUTION_") {
			environment[name] = value
		}
	}
	for name, value := range item.Environment {
		if !strings.HasPrefix(name, "LOOM_EXECUTION_") {
			environment[name] = value
		}
	}
	// Runtime-owned broker settings are appended last in canonical order and
	// cannot be overridden by an admitted task plan.
	for name, value := range trustedEnvironment {
		environment[name] = value
	}
	// The Python controller and Gateway use this same cutoff. Do not let
	// process startup reset the agent's full task-owned allowance.
	environment["LOOM_EXECUTION_PHASE_DEADLINE"] = strconv.FormatFloat(float64(deadline.UnixNano())/1e9, 'f', 9, 64)
	environment["LOOM_EXECUTION_TERMINATION_GRACE_SECONDS"] = strconv.FormatFloat(terminationGrace.Seconds(), 'f', -1, 64)
	environment["LOOM_EXECUTION_FINALIZATION_TIMEOUT_SECONDS"] = strconv.FormatInt(item.FinalizationTimeoutSeconds, 10)
	names := make([]string, 0, len(environment))
	for name := range environment {
		names = append(names, name)
	}
	sort.Strings(names)
	command.Env = make([]string, 0, len(names))
	for _, name := range names {
		command.Env = append(command.Env, name+"="+environment[name])
	}
	command.Stdout, command.Stderr = stdout, stderr
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	started := time.Now().UTC()
	// Direct script phases declare their report here. Workspace controllers
	// prepare reports in the private sandbox instead, through its trusted RPC.
	reportPrepared := false
	if declaresVerifierReport(item) {
		err = prepareVerifierReport(workspace, item.Environment["LOOM_VERIFIER_OUTPUT"])
		reportPrepared = err == nil
	}
	if err == nil {
		err = command.Start()
	}
	if err == nil {
		waited := make(chan error, 1)
		go func() { waited <- command.Wait() }()
		select {
		case err = <-waited:
		case <-phaseCtx.Done():
			_ = syscall.Kill(-command.Process.Pid, syscall.SIGTERM)
			cleanupBudget := terminationGrace
			if parent.Err() == nil && errors.Is(context.Cause(phaseCtx), context.DeadlineExceeded) {
				cleanupBudget += time.Duration(item.FinalizationTimeoutSeconds) * time.Second
			}
			timer := time.NewTimer(cleanupBudget)
			select {
			case <-waited:
				if !timer.Stop() {
					<-timer.C
				}
			case <-timer.C:
				_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
				<-waited
			}
			err = context.Cause(phaseCtx)
		}
	}
	finished := time.Now().UTC()
	evidence := phaseEvidence{
		verifierReportPrepared: reportPrepared,
		Role:                   item.Role, Ordinal: ordinal, StartedAt: started, FinishedAt: finished,
		// A moved deadline cancels with DeadlineExceeded as the cause, not Err().
		ExitCode: 0, TimedOut: errors.Is(context.Cause(phaseCtx), context.DeadlineExceeded) || !finished.Before(currentDeadline()),
		Stdout: stdout.evidence(filepath.Base(stdoutPath)), Stderr: stderr.evidence(filepath.Base(stderrPath)),
	}
	if err != nil {
		evidence.ExitCode = -1
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			evidence.ExitCode = exit.ExitCode()
		}
		if command.ProcessState != nil {
			evidence.ExitCode = command.ProcessState.ExitCode()
			if status, ok := command.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
				evidence.Signal = status.Signal().String()
			}
		}
	}
	return evidence, err
}

func declaresVerifierReport(item phase) bool {
	_, declared := item.Environment["LOOM_VERIFIER_OUTPUT"]
	return item.Role == "verifier" && declared
}

var cleanupDiagnosticSuffix = regexp.MustCompile(
	`^pid=[1-9][0-9]{0,9};ppid=[0-9]{1,10};state=[RSDTtXZPIUW];uid=[0-9]{1,10};wchan=[A-Za-z0-9_.]{1,64};kill_errno=[0-9]{1,3};sigkill_pending=[01](?:,pid=[1-9][0-9]{0,9};ppid=[0-9]{1,10};state=[RSDTtXZPIUW];uid=[0-9]{1,10};wchan=[A-Za-z0-9_.]{1,64};kill_errno=[0-9]{1,3};sigkill_pending=[01]){0,3}$`,
)

// The trusted Terminus controller writes this typed marker only after model
// execution and finalization have unwound. Task sandbox files are separate.
func agentTransportFailure(workspace string) string {
	root, err := os.OpenRoot(workspace)
	if err != nil {
		return ""
	}
	defer root.Close()
	file, err := root.Open(".loom/agent/transport-failure.json")
	if err != nil {
		return ""
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 4096 {
		return ""
	}
	payload, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil || len(payload) > 4096 {
		return ""
	}
	var marker struct {
		FailureReason string `json:"failure_reason"`
	}
	if err := json.Unmarshal(payload, &marker); err != nil {
		return ""
	}
	switch marker.FailureReason {
	case "gateway_error", "provider_transport_disconnect":
		return marker.FailureReason
	default:
		return ""
	}
}

func sandboxCleanupFailure(workspace, role string) bool {
	if role != "agent" && role != "verifier" {
		return false
	}
	path := filepath.Join(workspace, ".loom", role, "exception.json")
	payload, err := os.ReadFile(path)
	if err != nil || len(payload) > 4096 {
		return false
	}
	var recorded struct {
		ExceptionType    string `json:"exception_type"`
		ExceptionMessage string `json:"exception_message"`
	}
	if err := json.Unmarshal(payload, &recorded); err != nil {
		return false
	}
	return recorded.ExceptionType == "SandboxRPCError" && cleanupFailureMessage(recorded.ExceptionMessage)
}

func cleanupFailureMessage(message string) bool {
	const head = "sandbox stop_processes failed (HTTP 409; "
	for _, reason := range []string{"cleanup_timeout", "cleanup_signal_denied", "cleanup_signal_failed"} {
		if message == head+reason+")" {
			return true
		}
		prefix := head + reason + "; "
		rest, ok := strings.CutPrefix(message, prefix)
		if ok && strings.HasSuffix(rest, ")") && cleanupDiagnosticSuffix.MatchString(strings.TrimSuffix(rest, ")")) {
			return true
		}
	}
	return false
}

func classifyFailure(ctx context.Context, evidence phaseEvidence) string {
	if errors.Is(context.Cause(ctx), errSandboxLost) {
		return "runtime_error"
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		return "cancelled"
	}
	if evidence.TimedOut || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "timed_out"
	}
	switch evidence.Role {
	case "setup":
		return "setup_error"
	case "verifier":
		return "verifier_error"
	default:
		return "task_error"
	}
}

func writeResult(path string, result resultManifest) error {
	return writeJSONAtomic(path, result)
}

func writeTerminationSummary(
	path string,
	result resultManifest,
	output *outputCommitEvidence,
) error {
	summary := terminationSummary{
		SchemaVersion:         "loom.execution-termination-summary.v1",
		RuntimeContractSHA256: result.RuntimeContractSHA256,
		CommandIdentitySHA256: result.CommandIdentitySHA256,
		ExecutionRole:         result.ExecutionRole,
		Status:                result.Status,
		PartialEvidence:       result.PartialEvidence,
		PhaseCount:            len(result.Phases),
		FinishedAt:            result.FinishedAt,
		ResultPath:            "result.json",
	}
	if output != nil {
		summary.OutputCommitted = true
		summary.OutputUploadSessionID = output.UploadSessionID
		summary.OutputManifestSHA256 = output.ManifestSHA256
		summary.OutputMarkerSHA256 = output.CommittedMarkerSHA256
	}
	payload, err := json.Marshal(
		summary,
	)
	if err != nil {
		return err
	}
	if len(payload) > 4095 {
		return fmt.Errorf("termination summary exceeds Kubernetes bound")
	}
	// Kubernetes bind-mounts the termination message file itself.  It cannot be
	// replaced by rename, so write and fsync this small summary in place after
	// the full result manifest has already been atomically committed.
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(append(payload, '\n')); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func writeJSONAtomic(path string, value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	temporary := path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(append(payload, '\n')); err != nil {
		_ = file.Close()
		_ = os.Remove(temporary)
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(temporary)
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	return os.Rename(temporary, path)
}

func secureDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("required path is not a real directory: %s", path)
	}
	return nil
}

func isWithin(root, candidate string) bool {
	relative, err := filepath.Rel(root, candidate)
	return err == nil && relative != "." && relative != ".." && !filepath.IsAbs(relative) && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
