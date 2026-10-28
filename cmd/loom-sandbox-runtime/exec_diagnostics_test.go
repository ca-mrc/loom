package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestExec124DiagnosticsClassifyRealProcessesWithoutPrivateData(t *testing.T) {
	var diagnostics bytes.Buffer
	original := log.Writer()
	log.SetOutput(&diagnostics)
	t.Cleanup(func() { log.SetOutput(original) })
	for _, test := range []struct {
		name, command, reason string
		timeout               float64
		code                  int
		cancel                bool
	}{
		{"deadline", "sleep 30", "deadline", .1, 124, false},
		{"output_pipe_wait", "sleep 30 &", "output_pipe_wait", 3, 124, false},
		{"process_exit", "exit 124", "process_exit", 0, 124, false},
		{"cancelled", `: > "$EXEC_READY"; sleep 30`, "cancelled", 3, 124, true},
		{"other_exit_is_not_logged", "exit 7", "", 3, 7, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			diagnostics.Reset()
			ready := filepath.Join(t.TempDir(), "ready")
			request := execRequest{
				Argv: []string{"/bin/sh", "-c", "printf private-stdout-sentinel; " +
					"printf private-stderr-sentinel >&2; " + test.command},
				Env:     map[string]string{"PRIVATE": "private-env-sentinel", "EXEC_READY": ready},
				Timeout: test.timeout,
			}
			data, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			incoming := httptest.NewRequest("POST", "/exec", bytes.NewReader(data)).WithContext(ctx)
			response := httptest.NewRecorder()
			server := runtimeServer{maxTimeout: 3 * time.Second}
			if test.cancel {
				done := make(chan struct{})
				go func() { server.execute(response, incoming); close(done) }()
				defer func() {
					cancel()
					select {
					case <-done:
					case <-time.After(4 * time.Second):
						t.Error("cancelled exec did not finish")
					}
				}()
				deadline := time.Now().Add(2 * time.Second)
				for {
					if _, err := os.Stat(ready); err == nil {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("real child did not reach cancellation barrier")
					}
					time.Sleep(time.Millisecond)
				}
				cancel()
				select {
				case <-done:
				case <-time.After(4 * time.Second):
					t.Fatal("cancelled exec did not return")
				}
			} else {
				server.execute(response, incoming)
			}
			var result execResult
			if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if response.Code != 200 || result.Code != test.code {
				t.Fatalf("unexpected execution status: HTTP %d code %d", response.Code, result.Code)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(response.Body.Bytes(), &fields); err != nil || len(fields) != 5 {
				t.Fatal("exec response shape changed")
			}
			message := diagnostics.String()
			if strings.Contains(message, "private-") || strings.Contains(message, ready) {
				t.Fatal("exec diagnostic exposed command, environment, path or output")
			}
			if test.code != 124 {
				if message != "" {
					t.Fatal("non-124 exec unexpectedly logged")
				}
				return
			}
			pattern := regexp.MustCompile(`sandbox exec return_code=124 reason=([a-z_]+) timeout_sec=([0-9.e+\-]+) duration_sec=([0-9.e+\-]+)\n$`)
			match := pattern.FindStringSubmatch(message)
			if len(match) != 4 || match[1] != test.reason || strings.Count(message, "sandbox exec") != 1 {
				t.Fatalf("missing or incorrect safe reason: %q", message)
			}
			timeout, timeoutErr := strconv.ParseFloat(match[2], 64)
			duration, durationErr := strconv.ParseFloat(match[3], 64)
			expectedTimeout := test.timeout
			if expectedTimeout == 0 {
				expectedTimeout = 3
			}
			if timeoutErr != nil || durationErr != nil || timeout != expectedTimeout || duration != result.Duration {
				t.Fatal("diagnostic and response do not share the effective timeout and duration")
			}
		})
	}
}
