package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// blackholeReadConn drops encrypted bytes after a successful warm request.
// TLS stays open, so a later request never reaches HTTP parsing or a handler.
// Only the first accepted connection is affected; fresh connections are healthy.
type blackholeReadConn struct {
	net.Conn
	drop    atomic.Bool
	dropped atomic.Int64
}

func (c *blackholeReadConn) Read(p []byte) (int, error) {
	for {
		n, err := c.Conn.Read(p)
		if !c.drop.Load() || n == 0 {
			return n, err
		}
		c.dropped.Add(int64(n))
		if err != nil {
			return 0, err
		}
	}
}

type blackholeFirstListener struct {
	net.Listener
	mu       sync.Mutex
	first    *blackholeReadConn
	accepted int
}

func (l *blackholeFirstListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.accepted++
	if l.first == nil {
		l.first = &blackholeReadConn{Conn: conn}
		return l.first, nil
	}
	return conn, nil
}

func newConnectionTestBroker(t *testing.T, server *httptest.Server, leaseID string) (*workloadBroker, *http.Transport) {
	t.Helper()
	tokenFile := filepath.Join(t.TempDir(), "pod-token")
	if err := os.WriteFile(tokenFile, []byte("test-pod-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("LOOM_EXECUTION_BROKER_URL", server.URL+"/internal/service-execution")
	t.Setenv("LOOM_EXECUTION_LEASE_ID", leaseID)
	t.Setenv("LOOM_EXECUTION_GENERATION", "1")
	t.Setenv("LOOM_EXECUTION_ROLE", "attempt")
	t.Setenv("LOOM_EXECUTION_POD_TOKEN_FILE", tokenFile)
	broker, err := workloadBrokerFromEnvironment()
	if err != nil {
		t.Fatal(err)
	}
	client := broker.client
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		// Preserve the unfixed nil-transport behavior while installing
		// the test CA: cloning DefaultTransport retains its defaults.
		transport = http.DefaultTransport.(*http.Transport).Clone()
		client.Transport = transport
	}
	if transport.HTTP2 != nil {
		transport.HTTP2.SendPingTimeout = 100 * time.Millisecond
		transport.HTTP2.PingTimeout = 50 * time.Millisecond
	}
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	client.Timeout = 2 * time.Second
	t.Cleanup(transport.CloseIdleConnections)
	return broker, transport
}

func TestOutputPrepareRetryRecoversBlackholedPooledConnection(t *testing.T) {
	for _, protocol := range []int{1, 2} {
		t.Run(fmt.Sprintf("HTTP%d", protocol), func(t *testing.T) {
			const sessionID = "0194d739-8bec-7b7b-88f5-62f7cbd42cb5"
			const leaseID = "0194d739-8bec-7b7b-88f5-62f7cbd42cb6"
			uploadToken := strings.Repeat("u", 48)
			resultBody := []byte("{}")
			resultSHA := fmt.Sprintf("sha256:%x", sha256.Sum256(resultBody))
			var prepareCalls atomic.Int32
			var warmProtocol atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer test-pod-token" ||
					r.Header.Get("X-Loom-Execution-Lease-Id") != leaseID ||
					r.Header.Get("X-Loom-Execution-Generation") != "1" ||
					r.Header.Get("X-Loom-Execution-Role") != "attempt" {
					http.Error(w, "identity mismatch", http.StatusForbidden)
					return
				}
				switch r.URL.Path {
				case "/internal/service-execution/warm":
					warmProtocol.Store(int32(r.ProtoMajor))
					_, _ = w.Write([]byte(`{}`))
				case "/internal/service-execution/fresh-control":
					_, _ = w.Write([]byte(`{}`))
				case "/internal/service-execution/outputs/prepare":
					prepareCalls.Add(1)
					var request outputPrepare
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.Files) != 1 || request.Files[0].RelativePath != "result.json" || request.Files[0].SHA256 != resultSHA {
						http.Error(w, "invalid prepare", http.StatusBadRequest)
						return
					}
					w.WriteHeader(http.StatusCreated)
					_ = json.NewEncoder(w).Encode(map[string]any{
						"upload_session_id": sessionID, "upload_token": uploadToken,
						"files":            []uploadPlanFile{{FileIndex: 0, RelativePath: "result.json"}},
						"small_file_batch": smallFileBatchLimits{MaxFiles: 1, MaxTotalBytes: 2, MaxFileBytes: 2},
					})
				case "/internal/service-execution/outputs/" + sessionID + "/small-files":
					var batch struct {
						Files []smallOutputFile `json:"files"`
					}
					if err := json.NewDecoder(r.Body).Decode(&batch); err != nil || len(batch.Files) != 1 ||
						batch.Files[0].FileIndex != 0 || string(batch.Files[0].Content) != string(resultBody) ||
						r.Header.Get("X-Loom-Upload-Token") != uploadToken {
						http.Error(w, "small file mismatch", http.StatusBadRequest)
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"files": []verifiedOutputFile{{FileIndex: 0, SizeBytes: int64(len(resultBody)), SHA256: resultSHA}}})
				case "/internal/service-execution/outputs/" + sessionID + "/commit":
					if r.Header.Get("X-Loom-Upload-Token") != uploadToken {
						http.Error(w, "upload token mismatch", http.StatusForbidden)
						return
					}
					_ = json.NewEncoder(w).Encode(outputCommitEvidence{
						UploadSessionID: sessionID, ManifestSHA256: "sha256:" + strings.Repeat("1", 64),
						CommittedMarkerSHA256: "sha256:" + strings.Repeat("2", 64),
					})
				default:
					http.NotFound(w, r)
				}
			})
			server := httptest.NewUnstartedServer(handler)
			listener := &blackholeFirstListener{Listener: server.Listener}
			server.Listener = listener
			server.EnableHTTP2 = protocol == 2
			server.StartTLS()
			defer server.Close()

			broker, transport := newConnectionTestBroker(t, server, leaseID)
			client := broker.client

			if err := broker.doJSON(context.Background(), http.MethodGet, broker.endpoint("/warm"), nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			if got := warmProtocol.Load(); got != int32(protocol) {
				t.Fatalf("warm request negotiated HTTP%d, want HTTP%d", got, protocol)
			}
			listener.mu.Lock()
			first := listener.first
			listener.mu.Unlock()
			first.drop.Store(true)

			// An independent verified TLS client proves that the endpoint and fresh
			// connections remain healthy after the old connection is blackholed.
			controlTransport := transport.Clone()
			control := &workloadBroker{
				root: broker.root, identity: broker.identity, podTokenFile: broker.podTokenFile,
				client: &http.Client{Transport: controlTransport, Timeout: 2 * time.Second},
			}
			if err := control.doJSON(context.Background(), http.MethodGet, control.endpoint("/fresh-control"), nil, nil, nil); err != nil {
				t.Fatalf("fresh connection control failed: %v", err)
			}
			controlTransport.CloseIdleConnections()

			// Shorten test client/PING clocks. Exercise the real artifact prepare
			// retry, its existing backoff and outer cap; there is no test retry shim.
			client.Timeout = 400 * time.Millisecond
			var reused, fresh atomic.Int32
			trace := &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) {
				if info.Reused {
					reused.Add(1)
				} else {
					fresh.Add(1)
				}
			}}
			ctx, cancel := context.WithTimeout(httptrace.WithClientTrace(context.Background(), trace), 3*time.Second)
			defer cancel()
			started := time.Now()
			output := t.TempDir()
			if err := os.WriteFile(filepath.Join(output, "result.json"), resultBody, 0o600); err != nil {
				t.Fatal(err)
			}
			evidence, err := broker.commitOutputs(ctx, output, "request-id")
			t.Logf("protocol=HTTP%d elapsed=%s pooled=%d fresh=%d prepare_handlers=%d dropped_bytes=%d error=%v",
				protocol, time.Since(started), reused.Load(), fresh.Load(), prepareCalls.Load(), first.dropped.Load(), err)
			if first.dropped.Load() == 0 || reused.Load() == 0 {
				t.Fatal("fixture did not exercise the warmed blackholed connection")
			}
			if err != nil {
				t.Fatalf("artifact retry did not recover through a healthy new connection: %v", err)
			}
			if prepareCalls.Load() != 1 || fresh.Load() == 0 || evidence.UploadSessionID != sessionID {
				t.Fatalf("unexpected recovery: prepare handlers=%d fresh=%d evidence=%+v", prepareCalls.Load(), fresh.Load(), evidence)
			}
		})
	}
}

// A handler can be legitimately quiet far longer than the connection health
// window. Native HTTP/2 PING replies must preserve both concurrent streams.
func TestBrokerHTTP2HealthPreservesHealthySlowModelAndControl(t *testing.T) {
	const modelBody = `{"choices":[{"message":{"content":"complete"}}]}`
	var warmAddress atomic.Value
	var modelCalls, controlCalls, cancelled, changedConnection, cancelCalls atomic.Int32
	cancelStarted, cancelObserved := make(chan struct{}, 1), make(chan struct{}, 1)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 {
			t.Error("healthy stream did not use HTTP/2")
		}
		expectedAuth := "Bearer test-pod-token"
		if strings.HasPrefix(r.URL.Path, "/openai/") {
			expectedAuth = "Bearer test-model-token"
		}
		if r.Header.Get("Authorization") != expectedAuth {
			http.Error(w, "identity mismatch", http.StatusForbidden)
			return
		}
		body := `{}`
		switch r.URL.Path {
		case "/internal/service-execution/warm":
			warmAddress.Store(r.RemoteAddr)
		case "/openai/v1/responses":
			cancelCalls.Add(1)
			select {
			case cancelStarted <- struct{}{}:
			default:
			}
			<-r.Context().Done()
			select {
			case cancelObserved <- struct{}{}:
			default:
			}
			return
		case "/internal/service-execution/slow-control", "/openai/v1/chat/completions":
			if r.URL.Path == "/openai/v1/chat/completions" {
				modelCalls.Add(1)
				body = modelBody
			} else {
				controlCalls.Add(1)
			}
			if r.RemoteAddr != warmAddress.Load().(string) {
				changedConnection.Add(1)
			}
			timer := time.NewTimer(600 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-r.Context().Done():
				cancelled.Add(1)
				return
			}
		default:
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	defer server.Close()
	broker, _ := newConnectionTestBroker(t, server, "0194d739-8bec-7b7b-88f5-62f7cbd42cb6")
	if err := broker.doJSON(context.Background(), http.MethodGet, broker.endpoint("/warm"), nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	broker.token = "test-model-token"
	broker.expires = time.Now().Add(time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	proxy, stop, err := broker.startProxy(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stop() }()
	results := make(chan error, 2)
	started := time.Now()
	go func() { results <- broker.doJSON(ctx, http.MethodGet, broker.endpoint("/slow-control"), nil, nil, nil) }()
	go func() {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, proxy+"/openai/v1/chat/completions", strings.NewReader(`{}`))
		if err != nil {
			results <- err
			return
		}
		response, err := http.DefaultClient.Do(req)
		if err != nil {
			results <- err
			return
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err == nil && (response.StatusCode != http.StatusOK || string(body) != modelBody) {
			err = fmt.Errorf("incomplete model response: status=%d", response.StatusCode)
		}
		results <- err
	}()
	for range 2 {
		if err := <-results; err != nil {
			t.Error(err)
		}
	}
	if modelCalls.Load() != 1 || controlCalls.Load() != 1 || cancelled.Load() != 0 || changedConnection.Load() != 0 {
		t.Fatalf("healthy shared connection interrupted/replayed: model=%d control=%d cancelled=%d new_connections=%d", modelCalls.Load(), controlCalls.Load(), cancelled.Load(), changedConnection.Load())
	}
	t.Logf("healthy concurrent HTTP/2 model/control streams survived %s without response headers", time.Since(started))

	// A responsive connection must not turn caller cancellation into a new
	// health-check or retry budget; the original model request still cancels.
	callerCtx, stopCaller := context.WithCancel(ctx)
	callerResult := make(chan error, 1)
	go func() {
		request, err := http.NewRequestWithContext(callerCtx, http.MethodPost, proxy+"/openai/v1/responses", strings.NewReader(`{}`))
		if err != nil {
			callerResult <- err
			return
		}
		response, err := http.DefaultClient.Do(request)
		if response != nil {
			_ = response.Body.Close()
		}
		callerResult <- err
	}()
	select {
	case <-cancelStarted:
	case <-ctx.Done():
		t.Fatal("cancelled-model handler did not start")
	}
	cancelledAt := time.Now()
	stopCaller()
	select {
	case err := <-callerResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("caller cancellation changed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("caller cancellation was delayed")
	}
	select {
	case <-cancelObserved:
	case <-time.After(time.Second):
		t.Fatal("upstream did not observe caller cancellation")
	}
	if cancelCalls.Load() != 1 {
		t.Fatalf("cancelled model was replayed %d times", cancelCalls.Load())
	}
	t.Logf("caller cancellation reached healthy HTTP/2 upstream in %s", time.Since(cancelledAt))
}

func TestOutputRetryHonorsCancelledCommitContext(t *testing.T) {
	var prepareCalls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		prepareCalls.Add(1)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer server.Close()
	broker, _ := newConnectionTestBroker(t, server, "0194d739-8bec-7b7b-88f5-62f7cbd42cb6")
	output := t.TempDir()
	if err := os.WriteFile(filepath.Join(output, "result.json"), []byte(`{}`), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	_, err := broker.commitOutputs(ctx, output, "request-id")
	if !errors.Is(err, context.Canceled) || prepareCalls.Load() != 0 || time.Since(started) > time.Second {
		t.Fatalf("cancelled commit changed its cap: error=%v prepare=%d elapsed=%s", err, prepareCalls.Load(), time.Since(started))
	}
}
