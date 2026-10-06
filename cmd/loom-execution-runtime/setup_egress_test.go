package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// #2310: install sources are reachable only during setup; afterwards the
// task's own frozen policy applies, which for gateway-only tasks is nothing.

func TestSetupEgressPlanContract(t *testing.T) {
	setupPhase := phase{Role: "setup", Argv: []string{"true"}, WorkingDirectory: "/workspace", TimeoutSeconds: 2}
	registry := &webAllowlist{Kind: "web-allowlist", Destinations: []webDestination{{Host: "registry.npmjs.org", Protocol: "https"}}}

	legacy := testPlan("/workspace", phase{Role: "agent", Argv: []string{"true"}, WorkingDirectory: "/workspace", TimeoutSeconds: 2})
	raw, _ := json.Marshal(legacy)
	if strings.Contains(string(raw), "setup_egress") {
		t.Fatal("legacy plan bytes changed")
	}

	valid := legacy
	valid.Setup = []phase{setupPhase}
	valid.SetupEgress = registry
	valid.OutputDeclarations = []outputDeclaration{taskEgressOutput}
	raw, _ = json.Marshal(valid)
	decoded, err := decodePlan(raw)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.SetupEgress == nil || decoded.TaskEgress != nil {
		t.Fatal("setup egress must not become task egress")
	}

	for name, mutate := range map[string]func(*plan){
		"no setup phase":     func(p *plan) { p.Setup = nil },
		"no egress evidence": func(p *plan) { p.OutputDeclarations = nil },
		"invalid source": func(p *plan) {
			p.SetupEgress = &webAllowlist{Kind: "web-allowlist", Destinations: []webDestination{{Host: "localhost", Protocol: "https"}}}
		},
	} {
		broken := valid
		mutate(&broken)
		raw, _ := json.Marshal(broken)
		if _, err := decodePlan(raw); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestPhasedEgressPolicyFollowsThePhase(t *testing.T) {
	broker := &workloadBroker{}
	setup := &webAllowlist{Kind: "web-allowlist", Destinations: []webDestination{{Host: "registry.npmjs.org", Protocol: "https"}}}
	task := &webAllowlist{Kind: "web-allowlist", Destinations: []webDestination{{Host: "data.example.org", Protocol: "https"}}}
	install := webDestination{Host: "registry.npmjs.org", Protocol: "https"}
	data := webDestination{Host: "data.example.org", Protocol: "https"}

	gatewayOnly := phasedEgressPolicy{broker: broker, setup: setup}
	web := phasedEgressPolicy{broker: broker, setup: setup, task: task}
	if err := (phasedEgressPolicy{broker: broker}).validate(); err == nil {
		t.Fatal("empty phased policy accepted")
	}
	for _, role := range []string{"setup", "agent", "verifier", ""} {
		broker.setPhase(role, time.Now().Add(time.Minute))
		inSetup := role == "setup"
		if gatewayOnly.permits(install) != inSetup || web.permits(install) != inSetup {
			t.Errorf("%q: install source permitted=%v", role, !inSetup)
		}
		if gatewayOnly.permits(data) {
			t.Errorf("%q: gateway-only task reached a web destination", role)
		}
		if web.permits(data) != !inSetup {
			t.Errorf("%q: task policy applied during setup or missing afterwards", role)
		}
	}
}

func TestSetupEgressThroughTheProxyClosesWhenTheAgentStarts(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "agent-package")
	}))
	defer upstream.Close()
	var mu sync.Mutex
	phases := []string{}
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		phases = append(phases, r.Header.Get("X-Loom-Execution-Phase"))
		mu.Unlock()
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer ws.CloseNow()
		if _, _, err := ws.Read(r.Context()); err != nil {
			return
		}
		remote, err := net.Dial("tcp", strings.TrimPrefix(upstream.URL, "http://"))
		if err != nil {
			return
		}
		defer remote.Close()
		_ = ws.Write(r.Context(), websocket.MessageText, []byte(`{"status":"ready"}`))
		stream := websocket.NetConn(r.Context(), ws, websocket.MessageBinary)
		done := make(chan struct{}, 2)
		go func() { _, _ = io.Copy(remote, stream); done <- struct{}{} }()
		go func() { _, _ = io.Copy(stream, remote); done <- struct{}{} }()
		<-done
	}))
	defer gateway.Close()
	root, _ := url.Parse(gateway.URL + "/internal/service-execution")
	tokenFile := filepath.Join(t.TempDir(), "pod-token")
	if err := os.WriteFile(tokenFile, []byte("pod-token"), 0600); err != nil {
		t.Fatal(err)
	}
	broker := &workloadBroker{podTokenFile: tokenFile, root: root, identity: workloadIdentity{LeaseID: "lease-one", Generation: 1, ExecutionRole: "attempt"}, client: gateway.Client()}
	policy := phasedEgressPolicy{broker: broker, setup: &webAllowlist{Kind: "web-allowlist", Destinations: []webDestination{{Host: "registry.npmjs.org", Protocol: "http"}}}}
	broker.setPhase("setup", time.Now().Add(time.Minute))
	proxy, stop, err := broker.startTaskEgress(context.Background(), policy, "sha256:bound", io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	proxyURL, _ := url.Parse(proxy)
	transport := &http.Transport{Proxy: http.ProxyURL(proxyURL)}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second}

	response, err := client.Get("http://registry.npmjs.org/codex.tgz")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if string(body) != "agent-package" {
		t.Fatalf("setup download: %d %q", response.StatusCode, body)
	}

	broker.setPhase("agent", time.Now().Add(time.Minute))
	response, err = client.Get("http://registry.npmjs.org/codex.tgz")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden || response.Header.Get("X-Loom-Egress-Error") != "task_egress_destination_denied" {
		t.Fatalf("agent phase reached the install source: %d", response.StatusCode)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(phases) != 1 || phases[0] != "setup" {
		t.Fatalf("gateway saw phases %v", phases)
	}
}
