package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestTaskEgressRealHTTPAndTLSAndRedirectDenial(t *testing.T) {
	for _, secure := range []bool{false, true} {
		t.Run(map[bool]string{false: "http", true: "https"}[secure], func(t *testing.T) {
			started := make(chan struct{})
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/slow" {
					close(started)
					<-r.Context().Done()
					return
				}
				if r.URL.Path == "/large" {
					_, _ = io.WriteString(w, strings.Repeat("package", 20000))
					return
				}
				if r.URL.Path == "/auth" {
					http.Error(w, "login required", http.StatusUnauthorized)
					return
				}
				if r.URL.Path == "/redirect" {
					http.Redirect(w, r, "https://denied.example.org/", 302)
					return
				}
				if r.Host != "packages.example.org" {
					t.Errorf("host drift: %s", r.Host)
				}
				_, _ = io.WriteString(w, "downloaded-package")
			})
			var upstream *httptest.Server
			if secure {
				upstream = httptest.NewTLSServer(handler)
			} else {
				upstream = httptest.NewServer(handler)
			}
			defer upstream.Close()
			protocol := "http"
			if secure {
				protocol = "https"
			}
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/internal/service-execution/task-egress" {
					t.Errorf("unexpected task-egress path: %s", r.URL.Path)
					http.NotFound(w, r)
					return
				}
				if r.Header.Get("Authorization") != "Bearer pod-token" || r.Header.Get("X-Loom-Execution-Lease-Id") != "lease-one" || r.Header.Get("X-Loom-Runtime-Contract-SHA256") != "sha256:bound" {
					t.Error("missing authoritative identity")
				}
				ws, err := websocket.Accept(w, r, nil)
				if err != nil {
					t.Error(err)
					return
				}
				defer ws.CloseNow()
				kind, raw, err := ws.Read(r.Context())
				if err != nil {
					return
				}
				var destination webDestination
				if kind != websocket.MessageText || json.Unmarshal(raw, &destination) != nil || destination.Host != "packages.example.org" || destination.Protocol != protocol {
					t.Error("invalid destination")
					return
				}
				remote, err := net.Dial("tcp", strings.TrimPrefix(strings.TrimPrefix(upstream.URL, "http://"), "https://"))
				if err != nil {
					t.Error(err)
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
			// The actuator supplies the service-execution base, not the gateway origin.
			root, _ := url.Parse(gateway.URL + "/internal/service-execution")
			tokenFile := filepath.Join(t.TempDir(), "pod-token")
			if err := os.WriteFile(tokenFile, []byte("pod-token"), 0600); err != nil {
				t.Fatal(err)
			}
			broker := &workloadBroker{podTokenFile: tokenFile, root: root, identity: workloadIdentity{LeaseID: "lease-one", Generation: 1, ExecutionRole: "attempt"}, client: gateway.Client()}
			broker.setPhase("agent", time.Now().Add(time.Minute))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			proxy, stop, err := broker.startTaskEgress(ctx, &webAllowlist{Kind: "web-allowlist", Destinations: []webDestination{{Host: "packages.example.org", Protocol: protocol}}}, "sha256:bound", io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			defer stop()
			proxyURL, _ := url.Parse(proxy)
			transport := &http.Transport{Proxy: http.ProxyURL(proxyURL), TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} // fixture certificate only
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
			response, err := client.Get(protocol + "://packages.example.org/package")
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(response.Body)
			response.Body.Close()
			if string(body) != "downloaded-package" {
				t.Fatalf("bad body %q status %d", body, response.StatusCode)
			}
			response, err = client.Get(protocol + "://packages.example.org/redirect")
			if err == nil {
				response.Body.Close()
				t.Fatal("redirect escaped allowlist")
			}
			response, err = client.Get(protocol + "://packages.example.org/large")
			if err != nil {
				t.Fatal(err)
			}
			body, _ = io.ReadAll(response.Body)
			response.Body.Close()
			if len(body) != 140000 {
				t.Fatalf("truncated download %d", len(body))
			}
			response, err = client.Get(protocol + "://packages.example.org/auth")
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != 401 {
				t.Fatalf("upstream auth was rewritten: %d", response.StatusCode)
			}
			finished := make(chan struct{})
			go func() {
				response, err := client.Get(protocol + "://packages.example.org/slow")
				if err == nil {
					response.Body.Close()
				}
				close(finished)
			}()
			<-started
			broker.setPhase("agent", time.Time{})
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Fatal("phase completion did not cancel tunnel")
			}
			broker.setPhase("verifier", time.Now().Add(time.Minute))
			response, err = client.Get(protocol + "://packages.example.org/package")
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != 200 {
				t.Fatal("verifier phase cannot use proxy")
			}
			cancel()
			response, err = client.Get(protocol + "://packages.example.org/package")
			if err == nil && response.StatusCode == 200 {
				response.Body.Close()
				t.Fatal("cancelled tunnel remained usable")
			}
		})
	}
}

func TestWebEgressContractRejectsAmbiguousHostAndLegacyOmission(t *testing.T) {
	p := testPlan("/workspace", phase{Role: "agent", Argv: []string{"true"}, WorkingDirectory: "/workspace", TimeoutSeconds: 2})
	raw, _ := json.Marshal(p)
	if strings.Contains(string(raw), "task_egress") {
		t.Fatal("legacy bytes changed")
	}
	p.OutputDeclarations = []outputDeclaration{taskEgressOutput}
	allow := &webAllowlist{Kind: "web-allowlist", Destinations: []webDestination{{Host: "example.org", Protocol: "https"}}}
	p.TaskEgress = &storedTaskEgress{taskEgressPolicy: allow}
	raw, _ = json.Marshal(p)
	if _, err := decodePlan(raw); err != nil {
		t.Fatal(err)
	}
	for _, host := range []string{"localhost", "127.0.0.1", "EXAMPLE.org", "example.org.", "*.example.org", "a.svc.cluster.local"} {
		allow.Destinations[0].Host = host
		if allow.validate() == nil {
			t.Errorf("accepted %s", host)
		}
	}
}

func TestPublicWebAllowsValidatedHostsAndAllowlistDoesNotWiden(t *testing.T) {
	open := &publicWeb{Kind: "public-web"}
	if err := open.validate(); err != nil {
		t.Fatal(err)
	}
	if !open.permits(webDestination{Host: "pypi.org", Protocol: "https"}) {
		t.Fatal("public web rejected a public https host")
	}
	allow := &webAllowlist{Kind: "web-allowlist", Destinations: []webDestination{{Host: "registry.npmjs.org", Protocol: "https"}}}
	if allow.permits(webDestination{Host: "pypi.org", Protocol: "https"}) {
		t.Fatal("allowlist accepted an undeclared host")
	}
	p := testPlan("/workspace", phase{Role: "agent", Argv: []string{"true"}, WorkingDirectory: "/workspace", TimeoutSeconds: 2})
	p.OutputDeclarations = []outputDeclaration{taskEgressOutput}
	p.TaskEgress = &storedTaskEgress{taskEgressPolicy: open}
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodePlan(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !decoded.TaskEgress.permits(webDestination{Host: "files.pythonhosted.org", Protocol: "https"}) {
		t.Fatal("decoded public web lost its open host policy")
	}
}

func TestTaskProxyRejectsUnsupportedPortsAndAmbiguousAuthorities(t *testing.T) {
	for _, item := range []struct{ method, target, host string }{
		{"CONNECT", "packages.example.org:23", "packages.example.org:23"},
		{"CONNECT", "127.0.0.1:443", "127.0.0.1:443"},
		{"GET", "http://packages.example.org:443/package", "packages.example.org:443"},
		{"GET", "http://packages.example.org/package", "different.example.org"},
		{"GET", "http://user:password@packages.example.org/package", "packages.example.org"},
	} {
		target := item.target
		if item.method == http.MethodConnect {
			target = "https://" + target
		}
		request, err := http.NewRequest(item.method, target, nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Host = item.host
		if _, err := requestDestination(request); err == nil {
			t.Fatalf("accepted %s %s", item.method, item.target)
		}
	}
}

func TestTaskEgressEvidenceIsBoundedValidJSON(t *testing.T) {
	var output strings.Builder
	audit := taskEgressAudit{output: &output, limit: 1024}
	for i := 0; i < 50; i++ {
		audit.record(webDestination{"example.org", "https"}, "task_egress_completed")
	}
	if output.Len() > 1024 || !strings.Contains(output.String(), "task_egress_diagnostics_truncated") {
		t.Fatal("evidence bound missing")
	}
	for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
		if !json.Valid([]byte(line)) {
			t.Fatalf("invalid JSON line %q", line)
		}
	}
}

func TestTaskEgressDiagnosticsUseDeclaredBundleInventory(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	p := testPlan(workspace, phase{Role: "agent", Argv: []string{"/bin/true"}, WorkingDirectory: workspace, TimeoutSeconds: 2})
	p.OutputDeclarations = []outputDeclaration{taskEgressOutput}
	if err := os.MkdirAll(filepath.Join(workspace, filepath.Dir(taskEgressOutput.SourcePath)), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, taskEgressOutput.SourcePath), []byte("{\"outcome\":\"task_egress_completed\"}\n"), 0600); err != nil {
		t.Fatal(err)
	}
	result, err := runPlan(context.Background(), p, workspace, output, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := captureDeclaredOutputs(p, workspace, output, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Outputs) != 1 || result.Outputs[0].State != "captured" || result.Outputs[0].RelativePath != "diagnostics/task-egress.jsonl" {
		t.Fatalf("missing diagnostic evidence: %#v", result.Outputs)
	}
	if err := writeResult(filepath.Join(output, "result.json"), result); err != nil {
		t.Fatal(err)
	}
	files, err := inventoryOutputs(output)
	if err != nil {
		t.Fatal(err)
	}
	expected := map[string]bool{"result.json": true, "diagnostics/task-egress.jsonl": true}
	for _, phase := range result.Phases {
		expected[phase.Stdout.Path] = true
		expected[phase.Stderr.Path] = true
	}
	if len(files) != len(expected) {
		t.Fatalf("inventory drift: %#v", files)
	}
	for _, file := range files {
		if !expected[file.RelativePath] {
			t.Fatalf("undeclared upload file %s", file.RelativePath)
		}
	}
}
