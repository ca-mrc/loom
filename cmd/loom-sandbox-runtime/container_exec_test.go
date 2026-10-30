package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var (
	containerExecTestContainer = strings.Repeat("a", 64)
	containerExecTestID        = strings.Repeat("b", 64)
)

func containerExecFrame(stream byte, value []byte) []byte {
	header := make([]byte, 8, 8+len(value))
	header[0] = stream
	binary.BigEndian.PutUint32(header[4:], uint32(len(value)))
	return append(header, value...)
}

func containerExecTestClient(t *testing.T, handler http.HandlerFunc) *containerExecClient {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	transport := &http.Transport{
		DisableKeepAlives: true,
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", server.Listener.Addr().String())
		},
	}
	client := &containerExecClient{http: &http.Client{
		Transport:     transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}}
	t.Cleanup(client.close)
	return client
}

func containerExecInspection(code int) map[string]any {
	return map[string]any{"ID": containerExecTestID, "ContainerID": containerExecTestContainer,
		"Running": false, "ExitCode": code}
}

func containerExecHandler(t *testing.T, output []byte, inspection map[string]any,
	requests *atomic.Int32) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		switch r.URL.Path {
		case "/containers/" + containerExecTestContainer + "/exec":
			if r.Method != http.MethodPost {
				t.Error("create did not use POST")
			}
			var created struct {
				User         string
				AttachStdout bool
				AttachStderr bool
				Tty          bool
				Cmd          []string
			}
			if json.NewDecoder(r.Body).Decode(&created) != nil || created.User != "0:0" ||
				!created.AttachStdout || !created.AttachStderr || created.Tty ||
				len(created.Cmd) != 1 || created.Cmd[0] != "true" {
				t.Error("create changed the fixed user, command, or output contract")
			}
			w.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(w).Encode(map[string]string{"Id": containerExecTestID})
		case "/exec/" + containerExecTestID + "/start":
			var start struct{ Detach, Tty *bool }
			if r.Method != http.MethodPost || json.NewDecoder(r.Body).Decode(&start) != nil ||
				start.Detach == nil || *start.Detach || start.Tty == nil || *start.Tty {
				t.Error("start must attach without a TTY")
			}
			_, _ = w.Write(output)
		case "/exec/" + containerExecTestID + "/json":
			if r.Method != http.MethodGet {
				t.Error("inspect did not use GET")
			}
			_ = json.NewEncoder(w).Encode(inspection)
		default:
			t.Error("unexpected Docker request path")
			http.NotFound(w, r)
		}
	}
}

func TestContainerExecPreservesOutputAndTerminalExitCodes(t *testing.T) {
	for _, code := range []int{0, 75} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			var requests atomic.Int32
			frames := append(containerExecFrame(1, []byte("123")), containerExecFrame(2, []byte("diagnostic"))...)
			client := containerExecTestClient(t, containerExecHandler(t, frames, containerExecInspection(code), &requests))
			result, err := client.execute(context.Background(), containerExecTestContainer, []string{"true"})
			if err != nil || result.Code != code || string(result.Stdout) != "123" ||
				string(result.Stderr) != "diagnostic" || result.Truncated || result.Duration <= 0 || requests.Load() != 3 {
				t.Fatalf("terminal output or exit code lost: code=%d requests=%d err=%v", result.Code, requests.Load(), err)
			}
		})
	}
}

func TestContainerExecTruncatesOutputUsingExistingSandboxBounds(t *testing.T) {
	var requests atomic.Int32
	frames := append(containerExecFrame(1, bytes.Repeat([]byte("x"), maxOutput+1)), containerExecFrame(2, []byte("err"))...)
	client := containerExecTestClient(t, containerExecHandler(t, frames, containerExecInspection(0), &requests))
	result, err := client.execute(context.Background(), containerExecTestContainer, []string{"true"})
	if err != nil || result.Code != 0 || !result.Truncated || len(result.Stdout) != maxOutput || string(result.Stderr) != "err" {
		t.Fatalf("output cap was not preserved: err=%v size=%d truncated=%t", err, len(result.Stdout), result.Truncated)
	}
}

func TestContainerExecRejectsInvalidInputsBeforeAnyRequest(t *testing.T) {
	var requests atomic.Int32
	client := containerExecTestClient(t, func(w http.ResponseWriter, r *http.Request) { requests.Add(1) })
	for _, test := range []struct {
		id   string
		argv []string
	}{
		{"", []string{"true"}},
		{strings.Repeat("a", 63), []string{"true"}},
		{strings.Repeat("A", 64), []string{"true"}},
		{"../" + strings.Repeat("a", 61), []string{"true"}},
		{containerExecTestContainer, nil},
		{containerExecTestContainer, []string{""}},
		{containerExecTestContainer, []string{"true", "private\x00argument"}},
		{containerExecTestContainer, []string{strings.Repeat("x", containerExecInputLimit)}},
	} {
		_, err := client.execute(context.Background(), test.id, test.argv)
		if err == nil || strings.Contains(err.Error(), "private") {
			t.Fatal("invalid input was accepted or exposed")
		}
	}
	if requests.Load() != 0 {
		t.Fatal("invalid input reached the daemon")
	}
}

func TestContainerExecRejectsMalformedFramesWithoutInspecting(t *testing.T) {
	reserved := containerExecFrame(1, []byte("x"))
	reserved[1] = 1
	truncated := containerExecFrame(1, []byte("private-output"))
	truncated = truncated[:len(truncated)-1]
	for name, output := range map[string][]byte{
		"partial_header": {1, 0, 0}, "reserved": reserved,
		"unknown_stream":    containerExecFrame(3, []byte("private-daemon-error")),
		"truncated_payload": truncated,
	} {
		t.Run(name, func(t *testing.T) {
			var requests atomic.Int32
			client := containerExecTestClient(t, containerExecHandler(t, output, containerExecInspection(0), &requests))
			result, err := client.execute(context.Background(), containerExecTestContainer, []string{"true"})
			if err == nil || strings.Contains(err.Error(), "private") || len(result.Stdout) != 0 || requests.Load() != 2 {
				t.Fatal("malformed stream was published, leaked, or followed by inspect")
			}
		})
	}
}

func TestContainerExecRequiresValidCreatedIDAndBoundedJSON(t *testing.T) {
	for name, data := range map[string]string{
		"missing": `{}`, "short": `{"Id":"abc"}`, "malformed": `{`,
		"oversized": strings.Repeat("x", containerExecJSONLimit+1),
	} {
		t.Run(name, func(t *testing.T) {
			var requests atomic.Int32
			client := containerExecTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.WriteHeader(http.StatusCreated)
				_, _ = io.WriteString(w, data)
			})
			if _, err := client.execute(context.Background(), containerExecTestContainer, []string{"true"}); err == nil || requests.Load() != 1 {
				t.Fatal("invalid create result was followed by another request")
			}
		})
	}
}

func TestContainerExecRequiresExactTerminalInspection(t *testing.T) {
	for _, damage := range []string{"running", "missing_running", "missing_exit", "negative_exit", "large_exit", "exec_id", "container_id"} {
		t.Run(damage, func(t *testing.T) {
			inspection := containerExecInspection(0)
			switch damage {
			case "running":
				inspection["Running"] = true
			case "missing_running":
				delete(inspection, "Running")
			case "missing_exit":
				delete(inspection, "ExitCode")
			case "negative_exit":
				inspection["ExitCode"] = -1
			case "large_exit":
				inspection["ExitCode"] = 256
			case "exec_id":
				inspection["ID"] = strings.Repeat("c", 64)
			case "container_id":
				inspection["ContainerID"] = strings.Repeat("c", 64)
			}
			var requests atomic.Int32
			client := containerExecTestClient(t, containerExecHandler(t, containerExecFrame(1, []byte("123")), inspection, &requests))
			result, err := client.execute(context.Background(), containerExecTestContainer, []string{"true"})
			if err == nil || len(result.Stdout) != 0 || requests.Load() != 3 {
				t.Fatal("unconfirmed output or an invalid terminal identity was accepted")
			}
		})
	}
}

func TestContainerExecCancellationStopsEveryStageWithoutAnotherRequest(t *testing.T) {
	for _, stage := range []string{"create", "start", "output", "inspect"} {
		t.Run(stage, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var requests atomic.Int32
			base := containerExecHandler(t, nil, containerExecInspection(0), &requests)
			client := containerExecTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				current := "create"
				if strings.HasSuffix(r.URL.Path, "/start") {
					current = "start"
				} else if strings.HasSuffix(r.URL.Path, "/json") {
					current = "inspect"
				}
				if stage == current || stage == "output" && current == "start" {
					requests.Add(1)
					// Consume the request before waiting for disconnect: net/http
					// cannot background-read EOF while a request body is unread.
					_, _ = io.Copy(io.Discard, r.Body)
					r.Body.Close()
					if stage == "output" {
						w.WriteHeader(http.StatusOK)
						w.(http.Flusher).Flush()
					}
					cancel()
					select {
					case <-r.Context().Done():
					case <-time.After(time.Second):
						t.Error("cancellation did not close the active connection")
					}
					return
				}
				base(w, r)
			})
			_, err := client.execute(ctx, containerExecTestContainer, []string{"true"})
			expected := int32(1)
			if stage == "start" || stage == "output" {
				expected = 2
			} else if stage == "inspect" {
				expected = 3
			}
			if !errors.Is(err, context.Canceled) || requests.Load() != expected {
				t.Fatalf("cancellation was lost or another request was sent: err=%v requests=%d", err, requests.Load())
			}
		})
	}
}

func TestContainerExecSharesOneAggregateDeadlineAcrossCreateAndOutput(t *testing.T) {
	const budget = 500 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	var requests atomic.Int32
	var outputCancelled atomic.Bool
	base := containerExecHandler(t, nil, containerExecInspection(0), &requests)
	client := containerExecTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/exec") {
			select {
			case <-time.After(budget * 2 / 3):
			case <-r.Context().Done():
				return
			}
			base(w, r)
			return
		}
		requests.Add(1)
		if !strings.HasSuffix(r.URL.Path, "/start") {
			t.Error("request was sent after the aggregate deadline")
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		r.Body.Close()
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
			outputCancelled.Store(true)
		case <-time.After(budget * 2 / 3):
			t.Error("start/output did not retain the caller's original deadline")
		}
	})
	_, err := client.execute(ctx, containerExecTestContainer, []string{"true"})
	if !errors.Is(err, context.DeadlineExceeded) || requests.Load() != 2 {
		t.Fatalf("aggregate deadline was reset or followed by inspect: err=%v requests=%d", err, requests.Load())
	}
	// The server observes transport cancellation asynchronously after Do returns.
	deadline := time.Now().Add(time.Second)
	for !outputCancelled.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !outputCancelled.Load() {
		t.Fatal("deadline did not close the active response connection")
	}
}

func TestContainerExecAlreadyCancelledSendsNoRequestAndErrorsStayPrivate(t *testing.T) {
	var requests atomic.Int32
	client := containerExecTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "private-daemon-error", http.StatusInternalServerError)
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.execute(ctx, containerExecTestContainer, []string{"true"}); !errors.Is(err, context.Canceled) || requests.Load() != 0 {
		t.Fatal("already cancelled execution reached the daemon")
	}
	if _, err := client.execute(context.Background(), containerExecTestContainer, []string{"true"}); err == nil || strings.Contains(err.Error(), "private") || requests.Load() != 1 {
		t.Fatal("daemon error was exposed or retried")
	}
}

func TestContainerExecDoesNotFollowDaemonRedirects(t *testing.T) {
	var requests atomic.Int32
	client := containerExecTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Location", "http://private-location.invalid/command")
		w.WriteHeader(http.StatusTemporaryRedirect)
	})
	_, err := client.execute(context.Background(), containerExecTestContainer, []string{"true"})
	if err == nil || strings.Contains(err.Error(), "private") || requests.Load() != 1 {
		t.Fatal("daemon redirect was followed, exposed, or retried")
	}
}
