package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestModelTokenRefreshWaitHonorsCallerCancellation(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		close(started)
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_ = json.NewEncoder(w).Encode(tokenResponse{SchemaVersion: "loom.service-execution-token.v1", Token: "fixture", ExpiresAt: time.Now().Add(time.Hour)})
	}))
	defer gateway.Close()
	defer close(release)
	root, _ := url.Parse(gateway.URL + "/internal/service-execution")
	broker := &workloadBroker{root: root, client: gateway.Client()}
	firstDone := make(chan struct{})
	go func() { defer close(firstDone); _, _ = broker.currentToken(context.Background()) }()
	<-started
	ctx, cancel := context.WithCancel(context.Background())
	waiting := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() { close(waiting); _, err := broker.currentToken(ctx); secondDone <- err }()
	<-waiting
	cancel()
	select {
	case err := <-secondDone:
		if err != context.Canceled {
			t.Fatalf("cancelled refresh waiter: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled model request remained queued behind another request's token refresh")
	}
	// The cancelled waiter must not cancel the refresh still needed by its caller.
	select {
	case <-firstDone:
		t.Fatal("waiter cancelled the refresh owner")
	default:
	}
}

func TestModelProxyStreamsBeforeUpstreamCompletes(t *testing.T) {
	release := make(chan struct{})
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer gateway.Close()
	defer close(release)
	root, _ := url.Parse(gateway.URL + "/internal/service-execution")
	broker := &workloadBroker{root: root, client: gateway.Client(), token: "fixture", expires: time.Now().Add(time.Hour)}
	proxy, stop, err := broker.startProxy(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", proxy+"/v1/responses", strings.NewReader(`{"stream":true}`))
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("model proxy buffered the stream until completion: %v", err)
	}
	defer response.Body.Close()
	line, err := bufio.NewReader(response.Body).ReadString('\n')
	if err != nil || line != "data: first\n" {
		t.Fatalf("first streaming chunk: %q, %v", line, err)
	}
}

// The real loopback broker must not serialize model sends under the token lock
// or inherit the Python/guest-control defaults of 100/128 active connections.
func TestModelProxyHolds129RequestsAndRecoversAfterCancellation(t *testing.T) {
	testModelProxyHeldRequests(t, 129, false)
}

func TestModelProxyOpensAdditionalHTTP2Connections(t *testing.T) {
	// Go's HTTP/2 server advertises 250 streams per connection. The model
	// transport may establish another connection instead of queueing all keys.
	testModelProxyHeldRequests(t, 301, true)
}

func testModelProxyHeldRequests(t *testing.T, count int, useHTTP2 bool) {
	t.Helper()
	var peersMu sync.Mutex
	peers := map[string]bool{}
	allArrived, allCancelled := make(chan struct{}), make(chan struct{})
	var arrived, cancelled, minted atomic.Int32
	gateway := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if useHTTP2 && r.ProtoMajor != 2 {
			t.Error("model request did not use HTTP/2")
		}
		_, _ = io.Copy(io.Discard, r.Body)
		if strings.HasSuffix(r.URL.Path, "/token") {
			minted.Add(1)
			_ = json.NewEncoder(w).Encode(tokenResponse{SchemaVersion: "loom.service-execution-token.v1", Token: "fixture", ExpiresAt: time.Now().Add(480 * time.Second)})
			return
		}
		if r.Header.Get("Authorization") != "Bearer fixture" {
			t.Error("broker did not inject its own scoped credential")
			http.Error(w, "unauthorized", 401)
			return
		}
		if r.URL.Query().Get("recover") == "true" {
			_, _ = io.WriteString(w, "recovered")
			return
		}
		peersMu.Lock()
		peers[r.RemoteAddr] = true
		peersMu.Unlock()
		if arrived.Add(1) == int32(count) {
			close(allArrived)
		}
		<-r.Context().Done()
		if cancelled.Add(1) == int32(count) {
			close(allCancelled)
		}
	}))
	gateway.EnableHTTP2 = useHTTP2
	if useHTTP2 {
		gateway.StartTLS()
	} else {
		gateway.Start()
	}
	defer gateway.Close()
	root, _ := url.Parse(gateway.URL + "/internal/service-execution")
	broker := &workloadBroker{root: root, client: gateway.Client()}
	broker.setPhase("agent", time.Now().Add(30*time.Second))
	defer broker.setPhase("", time.Time{})
	proxy, stop, err := broker.startProxy(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, count)
	for i := 0; i < count; i++ {
		go func(i int) {
			path := "/v1/chat/completions"
			if i%2 == 0 {
				path = "/v1/responses"
			}
			req, _ := http.NewRequestWithContext(ctx, "POST", proxy+path, strings.NewReader("{}"))
			response, err := http.DefaultClient.Do(req)
			if err == nil {
				response.Body.Close()
			}
			done <- err
		}(i)
	}
	select {
	case <-allArrived:
	case <-ctx.Done():
		t.Fatalf("only %d/%d model sends reached held upstream", arrived.Load(), count)
	}
	peersMu.Lock()
	connections := len(peers)
	peersMu.Unlock()
	if useHTTP2 && connections < 2 {
		t.Errorf("%d held requests failed to cross the peer stream cap: %d connections", count, connections)
	}
	cancel()
	for i := 0; i < count; i++ {
		if err := <-done; err == nil {
			t.Error("cancelled held request returned success")
		}
	}
	select {
	case <-allCancelled:
	case <-time.After(time.Second):
		t.Fatalf("only %d/%d cancelled upstreams released", cancelled.Load(), count)
	}
	response, err := http.Post(proxy+"/v1/chat/completions?recover=true", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != 200 || string(body) != "recovered" {
		t.Fatalf("recovery failed: status=%d body=%s err=%v", response.StatusCode, body, err)
	}
	if minted.Load() != 1 {
		t.Fatalf("concurrent model sends minted %d credentials instead of reusing one scoped refresh", minted.Load())
	}
}
