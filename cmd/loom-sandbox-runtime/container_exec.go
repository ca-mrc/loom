package main

// This client is for the trusted guest controller, never a service-container
// socket mount or a task-selected Docker endpoint. The caller owns the complete
// operation deadline and the inner command's activation/cleanup protocol.

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
)

const (
	containerExecSocket     = "/var/run/docker.sock"
	containerExecJSONLimit  = 64 * 1024
	containerExecInputLimit = 1024 * 1024
)

type containerExecError struct {
	stage, reason string
	cause         error
}

func (failure *containerExecError) Error() string {
	// Both fields are fixed internal labels; no daemon errors or command data.
	return "container_exec_" + failure.stage + "_" + failure.reason
}

func (failure *containerExecError) Unwrap() error { return failure.cause }

func containerExecFailure(ctx context.Context, stage, reason string) error {
	if ctx != nil {
		switch ctx.Err() {
		case context.DeadlineExceeded:
			return &containerExecError{stage, "deadline", context.DeadlineExceeded}
		case context.Canceled:
			return &containerExecError{stage, "cancelled", context.Canceled}
		}
	}
	return &containerExecError{stage: stage, reason: reason}
}

type containerExecClient struct {
	http *http.Client
}

func newContainerExecClient() *containerExecClient {
	transport := &http.Transport{
		// Do not replay a request on a stale reused connection. These bounded
		// operations connect only to the same guest's local Unix socket.
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", containerExecSocket)
		},
	}
	return &containerExecClient{http: &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}}
}

func (client *containerExecClient) close() {
	if client != nil && client.http != nil {
		client.http.CloseIdleConnections()
	}
}

func validContainerExecID(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !('0' <= character && character <= '9' || 'a' <= character && character <= 'f') {
			return false
		}
	}
	return true
}

func (client *containerExecClient) request(ctx context.Context, method, path, stage string,
	body []byte, expectedStatus int) (*http.Response, error) {
	if ctx.Err() != nil {
		return nil, containerExecFailure(ctx, stage, "request_failed")
	}
	request, err := http.NewRequestWithContext(ctx, method, "http://docker"+path, bytes.NewReader(body))
	if err != nil {
		return nil, containerExecFailure(ctx, stage, "request_invalid")
	}
	request.Header.Set("Content-Type", "application/json")
	// Even an injected transport must not turn a request into a replayable
	// write. The production transport also avoids reused-connection retries.
	request.GetBody = nil
	response, err := client.http.Do(request)
	if err != nil {
		if response != nil && response.Body != nil {
			response.Body.Close()
		}
		return nil, containerExecFailure(ctx, stage, "request_failed")
	}
	if response.StatusCode != expectedStatus {
		response.Body.Close()
		return nil, containerExecFailure(ctx, stage, "status_invalid")
	}
	return response, nil
}

func (client *containerExecClient) document(ctx context.Context, method, path, stage string,
	body []byte, expectedStatus int) ([]byte, error) {
	response, err := client.request(ctx, method, path, stage, body, expectedStatus)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, containerExecJSONLimit+1))
	if err != nil || len(data) > containerExecJSONLimit || ctx.Err() != nil {
		return nil, containerExecFailure(ctx, stage, "response_invalid")
	}
	return data, nil
}

func readContainerExecOutput(source io.Reader) (stdout, stderr cappedBuffer, err error) {
	var header [8]byte
	for {
		count, failure := io.ReadFull(source, header[:])
		if failure == io.EOF && count == 0 {
			return stdout, stderr, nil
		}
		if failure != nil || header[1] != 0 || header[2] != 0 || header[3] != 0 {
			return stdout, stderr, errors.New("container_exec_output_invalid")
		}
		var destination io.Writer
		switch header[0] {
		case 1:
			destination = &stdout
		case 2:
			destination = &stderr
		default:
			return stdout, stderr, errors.New("container_exec_output_invalid")
		}
		// Copy through the existing capped writer without allocating from an
		// untrusted frame length. Excess output is drained under the same caller
		// context and marked truncated, just like buffered sandbox execution.
		if _, failure := io.CopyN(destination, source, int64(binary.BigEndian.Uint32(header[4:]))); failure != nil {
			return stdout, stderr, errors.New("container_exec_output_invalid")
		}
	}
}

func (client *containerExecClient) execute(ctx context.Context, containerID string,
	argv []string) (result execResult, failure error) {
	started := time.Now()
	defer func() { result.Duration = time.Since(started).Seconds() }()
	if ctx == nil || client == nil || client.http == nil || !validContainerExecID(containerID) ||
		len(argv) == 0 || argv[0] == "" {
		return execResult{}, containerExecFailure(ctx, "input", "invalid")
	}
	for _, argument := range argv {
		if strings.ContainsRune(argument, '\x00') {
			return execResult{}, containerExecFailure(ctx, "input", "invalid")
		}
	}
	create, err := json.Marshal(struct {
		User         string
		AttachStdout bool
		AttachStderr bool
		Tty          bool
		Cmd          []string
	}{"0:0", true, true, false, argv})
	if err != nil || len(create) > containerExecInputLimit {
		return execResult{}, containerExecFailure(ctx, "input", "invalid")
	}
	data, err := client.document(ctx, http.MethodPost, "/containers/"+containerID+"/exec",
		"create", create, http.StatusCreated)
	if err != nil {
		return execResult{}, err
	}
	var created struct {
		ID string `json:"Id"`
	}
	if json.Unmarshal(data, &created) != nil || !validContainerExecID(created.ID) {
		return execResult{}, containerExecFailure(ctx, "create", "response_invalid")
	}
	response, err := client.request(ctx, http.MethodPost, "/exec/"+created.ID+"/start",
		"start", []byte(`{"Detach":false,"Tty":false}`), http.StatusOK)
	if err != nil {
		return execResult{}, err
	}
	stdout, stderr, err := readContainerExecOutput(response.Body)
	response.Body.Close()
	if err != nil || ctx.Err() != nil {
		return execResult{}, containerExecFailure(ctx, "output", "response_invalid")
	}
	data, err = client.document(ctx, http.MethodGet, "/exec/"+created.ID+"/json",
		"inspect", nil, http.StatusOK)
	if err != nil {
		return execResult{}, err
	}
	var inspected struct {
		ID          string `json:"ID"`
		ContainerID string `json:"ContainerID"`
		Running     *bool  `json:"Running"`
		ExitCode    *int   `json:"ExitCode"`
	}
	if json.Unmarshal(data, &inspected) != nil || inspected.ID != created.ID ||
		inspected.ContainerID != containerID || inspected.Running == nil || *inspected.Running ||
		inspected.ExitCode == nil || *inspected.ExitCode < 0 || *inspected.ExitCode > 255 || ctx.Err() != nil {
		return execResult{}, containerExecFailure(ctx, "inspect", "response_invalid")
	}
	return execResult{Code: *inspected.ExitCode, Stdout: stdout.buffer.Bytes(), Stderr: stderr.buffer.Bytes(),
		Truncated: stdout.truncated || stderr.truncated}, nil
}

func (s runtimeServer) executeContainer(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Container string   `json:"container"`
		Argv      []string `json:"argv"`
		Timeout   float64  `json:"timeout_sec"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, containerExecInputLimit))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || !validContainerExecID(request.Container) ||
		len(request.Argv) == 0 || request.Argv[0] == "" {
		w.Header().Set("X-Loom-Sandbox-Error", "exec_request_invalid")
		http.Error(w, "invalid container exec request", http.StatusBadRequest)
		return
	}
	for _, argument := range request.Argv {
		if strings.ContainsRune(argument, '\x00') {
			w.Header().Set("X-Loom-Sandbox-Error", "exec_request_invalid")
			http.Error(w, "invalid container exec request", http.StatusBadRequest)
			return
		}
	}
	deadline, _, rejection := s.validateCommand(execRequest{Argv: request.Argv, Timeout: request.Timeout})
	if rejection != nil {
		rejection.write(w)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), deadline)
	defer cancel()
	result, err := s.containerExec.execute(ctx, request.Container, request.Argv)
	if err != nil {
		if ctx.Err() == nil {
			w.Header().Set("X-Loom-Sandbox-Error", "container_exec_failed")
			http.Error(w, "unable to execute container control", http.StatusBadGateway)
			return
		}
		result.Code = 124
		reason := "cancelled"
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			reason = "deadline"
		}
		var failure *containerExecError
		stage := "request"
		if errors.As(err, &failure) {
			stage = failure.stage
		}
		log.Printf("sandbox container exec return_code=124 reason=%s stage=%s timeout_sec=%g duration_sec=%g",
			reason, stage, deadline.Seconds(), result.Duration)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(result)
}
