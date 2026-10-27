package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const liveTestTrial = "0194d739-8bec-7b7b-88f5-62f7cbd42cb3"

func liveTestBroker(t *testing.T, receive func(nativeTrajectoryRequest)) (*workloadBroker, func()) {
	t.Helper()
	tokenFile := filepath.Join(t.TempDir(), "pod-token")
	if err := os.WriteFile(tokenFile, []byte("only-the-controller-token"), 0600); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer only-the-controller-token" || r.Header.Get("X-Loom-Execution-Lease-Id") != "bound-lease" || r.Header.Get("X-Loom-Execution-Generation") != "7" || r.Header.Get("X-Loom-Execution-Role") != "attempt" {
			t.Error("preview request lost immutable Pod identity")
		}
		if r.URL.Path == "/internal/service-execution/llm-calls" && r.Method == http.MethodGet {
			_, _ = fmt.Fprintf(w, `{"trial_id":%q,"team_id":"bound-team","step_id":"agent","task_id":"tiny-task","agent_name":"oracle","items":[]}`, liveTestTrial)
			return
		}
		if r.URL.Path != "/internal/service-execution/trajectory-events" || r.Method != http.MethodPost {
			t.Errorf("unexpected route %s %s", r.Method, r.URL.Path)
			w.WriteHeader(404)
			return
		}
		var body nativeTrajectoryRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		if body.SchemaVersion != "loom.service-execution-trajectory-events.v1" || body.LeaseID != "bound-lease" || body.Generation != 7 || body.ExecutionRole != "attempt" {
			t.Error("body identity not broker bound")
		}
		receive(body)
		var last struct {
			Seq int64 `json:"seq"`
		}
		_ = json.Unmarshal(body.Events[len(body.Events)-1], &last)
		_, _ = fmt.Fprintf(w, `{"inserted":%d,"next_after_seq":%d}`, len(body.Events), last.Seq)
	}))
	root, _ := url.Parse(server.URL + "/internal/service-execution")
	return &workloadBroker{root: root, client: server.Client(), podTokenFile: tokenFile, identity: workloadIdentity{LeaseID: "bound-lease", Generation: 7, ExecutionRole: "attempt"}}, server.Close
}

func fastLiveOptions() nativeTrajectoryOptions {
	o := defaultNativeTrajectoryOptions()
	o.pollInterval = 10 * time.Millisecond
	o.requestTimeout = 100 * time.Millisecond
	o.stopTimeout = 300 * time.Millisecond
	return o
}

func TestNativeTrajectoryStreamsPhaseAndPartialAppendBeforeProcessFinishes(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	directory := filepath.Join(workspace, ".loom", "agent")
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var received []map[string]any
	broker, stop := liveTestBroker(t, func(body nativeTrajectoryRequest) {
		mu.Lock()
		defer mu.Unlock()
		for _, raw := range body.Events {
			var event map[string]any
			if err := json.Unmarshal(raw, &event); err != nil {
				t.Error(err)
			}
			received = append(received, event)
		}
	})
	defer stop()
	agent := phase{Role: "agent", Argv: []string{"/bin/sh", "-c", `while [ ! -f release ]; do sleep 0.01; done`}, WorkingDirectory: workspace, TimeoutSeconds: 5, Environment: map[string]string{}}
	p := testPlan(workspace, agent)
	p.OutputDeclarations = []outputDeclaration{{SourcePath: ".loom/agent/trajectory.jsonl", RelativePath: "trajectory/events.jsonl", Kind: "trajectory"}}
	uploader := startNativeTrajectory(context.Background(), broker, p, workspace, fastLiveOptions())
	defer uploader.close()
	done := make(chan error, 1)
	go func() {
		result, err := runPlan(context.Background(), p, workspace, output, nil, uploader.phaseBoundary)
		if result.Status != "succeeded" && err == nil {
			err = fmt.Errorf("result status %s", result.Status)
		}
		done <- err
	}()
	waitLive(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range received {
			if e["kind"] == "step_start" {
				return true
			}
		}
		return false
	})
	select {
	case <-done:
		t.Fatal("process finished before live phase delivery")
	default:
	}
	raw := fmt.Sprintf(`{"kind":"agent_thought","trial_id":%q,"step_id":"agent","seq":0,"emitted_at":"2026-10-27T00:00:00Z","content":"safe preview"}`, liveTestTrial)
	f, err := os.OpenFile(filepath.Join(directory, "trajectory.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString(raw[:len(raw)/2])
	time.Sleep(35 * time.Millisecond)
	mu.Lock()
	for _, e := range received {
		if e["kind"] == "agent_thought" {
			t.Error("partial JSONL line was published")
		}
	}
	mu.Unlock()
	_, _ = f.WriteString(raw[len(raw)/2:] + "\n")
	_ = f.Close()
	waitLive(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range received {
			if e["kind"] == "agent_thought" {
				return true
			}
		}
		return false
	})
	select {
	case <-done:
		t.Fatal("raw append was not live before process completion")
	default:
	}
	if err := os.WriteFile(filepath.Join(workspace, "release"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	uploader.close()
	original, err := os.ReadFile(filepath.Join(directory, "trajectory.jsonl"))
	if err != nil || string(original) != raw+"\n" {
		t.Fatal("canonical local trajectory was changed")
	}
	mu.Lock()
	defer mu.Unlock()
	for n, e := range received {
		if e["trial_id"] != liveTestTrial || int(e["seq"].(float64)) != n {
			t.Fatalf("noncontiguous or foreign preview identity: %#v", e)
		}
	}
	if received[len(received)-1]["kind"] != "step_end" {
		t.Fatal("real phase end missing")
	}
}

func waitLive(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("live event did not arrive before process completion")
}

func TestNativeTrajectoryRejectsForeignIdentityAndUndeclaredOrSymlinkPaths(t *testing.T) {
	for _, mode := range []string{"foreign-trial", "undeclared", "symlink-parent", "oversize"} {
		t.Run(mode, func(t *testing.T) {
			workspace, outside := t.TempDir(), t.TempDir()
			dir := filepath.Join(workspace, ".loom", "agent")
			if err := os.MkdirAll(dir, 0700); err != nil {
				t.Fatal(err)
			}
			trial := liveTestTrial
			if mode == "foreign-trial" {
				trial = "0194d739-8bec-7b7b-88f5-62f7cbd42cb4"
			}
			raw := fmt.Sprintf(`{"kind":"agent_thought","trial_id":%q,"step_id":"agent","seq":0,"emitted_at":"2026-10-27T00:00:00Z","content":"forbidden"}`, trial)
			if mode == "oversize" {
				raw = strings.Repeat("x", nativeTrajectoryMaxEventBytes+1)
			}
			if mode == "symlink-parent" {
				if err := os.RemoveAll(filepath.Join(workspace, ".loom")); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(filepath.Join(outside, "agent"), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(outside, filepath.Join(workspace, ".loom")); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(dir, "trajectory.jsonl"), []byte(raw+"\n"), 0600); err != nil {
				t.Fatal(err)
			}
			var mu sync.Mutex
			found := false
			broker, stop := liveTestBroker(t, func(b nativeTrajectoryRequest) {
				mu.Lock()
				defer mu.Unlock()
				for _, e := range b.Events {
					if strings.Contains(string(e), "forbidden") {
						found = true
					}
				}
			})
			defer stop()
			p := testPlan(workspace, phase{Role: "agent"})
			kind := "trajectory"
			if mode == "undeclared" {
				kind = "task_artifact"
			}
			p.OutputDeclarations = []outputDeclaration{{SourcePath: ".loom/agent/trajectory.jsonl", Kind: kind, RelativePath: "trajectory/events.jsonl"}}
			u := startNativeTrajectory(context.Background(), broker, p, workspace, fastLiveOptions())
			u.phaseBoundary("agent", time.Now().Add(time.Second))
			time.Sleep(65 * time.Millisecond)
			u.close()
			mu.Lock()
			defer mu.Unlock()
			if found {
				t.Fatal("unsafe file/event was published")
			}
		})
	}
}

func TestNativeTrajectoryOutageDoesNotBlockExecutionOrShutdown(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	root, _ := url.Parse(server.URL)
	broker := &workloadBroker{root: root, client: server.Client(), identity: workloadIdentity{ExecutionRole: "attempt"}}
	workspace, output := t.TempDir(), t.TempDir()
	p := testPlan(workspace, phase{Role: "agent", Argv: []string{"/bin/sh", "-c", "true"}, WorkingDirectory: workspace, TimeoutSeconds: 2})
	options := fastLiveOptions()
	options.requestTimeout = 500 * time.Millisecond
	options.stopTimeout = 40 * time.Millisecond
	u := startNativeTrajectory(context.Background(), broker, p, workspace, options)
	start := time.Now()
	result, err := runPlan(context.Background(), p, workspace, output, nil, u.phaseBoundary)
	u.close()
	if err != nil || result.Status != "succeeded" {
		t.Fatalf("preview outage changed execution: %v %#v", err, result)
	}
	if time.Since(start) > 300*time.Millisecond {
		t.Fatal("preview network delayed execution/shutdown")
	}
}

func TestNativeTrajectoryRetriesExactBatchWithoutSequenceGaps(t *testing.T) {
	var requests atomic.Int32
	var mu sync.Mutex
	var first []byte
	var last int64 = -1
	accepted := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			_, _ = fmt.Fprintf(w, `{"trial_id":%q,"step_id":"agent","task_id":"tiny-task","agent_name":"oracle"}`, liveTestTrial)
			return
		}
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		defer mu.Unlock()
		n := requests.Add(1)
		if n == 1 {
			first = append([]byte(nil), body...)
			http.Error(w, "unavailable", 503)
			return
		}
		if n == 2 && !bytes.Equal(first, body) {
			t.Error("retry mutated or replaced an unconfirmed contiguous batch")
		}
		var req nativeTrajectoryRequest
		if err := json.Unmarshal(body, &req); err != nil {
			t.Error(err)
			return
		}
		for _, raw := range req.Events {
			var event struct {
				Seq int64 `json:"seq"`
			}
			_ = json.Unmarshal(raw, &event)
			if event.Seq != last+1 {
				t.Errorf("seq gap: %d after %d", event.Seq, last)
			}
			last = event.Seq
		}
		_, _ = fmt.Fprintf(w, `{"inserted":%d,"next_after_seq":%d}`, len(req.Events), last)
		select {
		case accepted <- struct{}{}:
		default:
		}
	}))
	defer server.Close()
	root, _ := url.Parse(server.URL + "/internal/service-execution")
	b := &workloadBroker{root: root, client: server.Client(), identity: workloadIdentity{ExecutionRole: "attempt"}}
	workspace := t.TempDir()
	p := testPlan(workspace, phase{Role: "agent"})
	u := startNativeTrajectory(context.Background(), b, p, workspace, fastLiveOptions())
	defer u.close()
	u.phaseBoundary("agent", time.Now().Add(time.Second))
	select {
	case <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("bounded retry did not recover")
	}
	u.phaseBoundary("agent", time.Time{})
	u.close()
	mu.Lock()
	defer mu.Unlock()
	if last < 2 {
		t.Fatal("phase end did not follow recovered batch")
	}
}

func TestNativeTrajectoryMemoryAndPhaseQueuesAreBounded(t *testing.T) {
	u := &nativeTrajectory{boundaries: make(chan nativeTrajectoryBoundary, 64), totalLimit: nativeTrajectoryMaxTotalBytes}
	for n := 0; n < nativeTrajectoryMaxQueueEvents+10; n++ {
		u.enqueue(map[string]any{"kind": "step_end", "trial_id": liveTestTrial, "step_id": "agent", "emitted_at": time.Now().UTC()})
	}
	if len(u.queue) > nativeTrajectoryMaxQueueEvents || !u.disabled {
		t.Fatal("preview outage can grow the event queue without bound")
	}
	for n := 0; n < 1000; n++ {
		u.phaseBoundary("agent", time.Now())
	}
	if !u.overflow.Load() || len(u.boundaries) != 64 {
		t.Fatal("phase notifications were not bounded/nonblocking")
	}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer server.Close()
	root, _ := url.Parse(server.URL)
	b := &workloadBroker{root: root, client: server.Client(), identity: workloadIdentity{ExecutionRole: "verifier"}}
	p := testPlan(t.TempDir(), phase{Role: "verifier"})
	p.ExecutionRole = "verifier"
	verifier := startNativeTrajectory(context.Background(), b, p, t.TempDir(), fastLiveOptions())
	verifier.phaseBoundary("verifier", time.Now())
	verifier.close()
	if verifier != nil || calls.Load() != 0 {
		t.Fatal("verifier-only runtime acquired primary preview authority")
	}
}

func TestNativeTrajectoryStopsAtDeclaredCumulativeBudget(t *testing.T) {
	u := &nativeTrajectory{totalLimit: 220}
	event := map[string]any{"kind": "step_end", "trial_id": liveTestTrial, "step_id": "agent", "emitted_at": "2026-10-27T00:00:00Z"}
	u.enqueue(event)
	if len(u.queue) != 1 || u.disabled {
		t.Fatal("first bounded event was not accepted")
	}
	// A successful flush releases pending memory, never the total output budget.
	u.queue = nil
	u.queueBytes = 0
	u.enqueue(event)
	if !u.disabled || u.sequence != 1 || len(u.queue) != 0 {
		t.Fatal("successful uploads bypassed the immutable cumulative budget")
	}
}

func TestNativeTrajectoryCloseDrainsMoreThanOneBatch(t *testing.T) {
	workspace := t.TempDir()
	directory := filepath.Join(workspace, ".loom", "agent")
	if err := os.MkdirAll(directory, 0700); err != nil {
		t.Fatal(err)
	}
	var body strings.Builder
	for n := 0; n < 100; n++ {
		fmt.Fprintf(&body, "{\"kind\":\"agent_thought\",\"trial_id\":%q,\"step_id\":\"agent\",\"seq\":%d,\"emitted_at\":\"2026-10-27T00:00:00Z\",\"content\":\"tail\"}\n", liveTestTrial, n)
	}
	if err := os.WriteFile(filepath.Join(directory, "trajectory.jsonl"), []byte(body.String()), 0600); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	received := 0
	batches := 0
	broker, stop := liveTestBroker(t, func(request nativeTrajectoryRequest) {
		mu.Lock()
		defer mu.Unlock()
		received += len(request.Events)
		batches++
		if len(request.Events) > 50 {
			t.Error("oversize event batch")
		}
	})
	defer stop()
	p := testPlan(workspace, phase{Role: "agent"})
	p.MaxArtifactBytes = 1024 * 1024
	p.OutputDeclarations = []outputDeclaration{{Kind: "trajectory", SourcePath: ".loom/agent/trajectory.jsonl", RelativePath: "trajectory/events.jsonl"}}
	u := startNativeTrajectory(context.Background(), broker, p, workspace, fastLiveOptions())
	u.phaseBoundary("agent", time.Now().Add(time.Second))
	u.phaseBoundary("agent", time.Time{})
	u.close()
	mu.Lock()
	defer mu.Unlock()
	if received != 103 || batches < 3 {
		t.Fatalf("shutdown left a valid tail queued: events=%d batches=%d", received, batches)
	}
}
