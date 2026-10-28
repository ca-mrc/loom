package main

import (
	"bytes"
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
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func mainlandTestProfile() *mainlandProxyProfile {
	return &mainlandProxyProfile{SchemaVersion: "loom.mainland-network-profile.v1", ProviderVersion: "fixture-20261028", GFWSuffixes: []string{"github.com", "githubusercontent.com"}, GFWHosts: []string{"exact.example.org"}, GreatFireHosts: []string{"gitlab.com"}, UnknownAction: "direct", IntermittentAction: "reject", Sources: map[string]any{"source": "fixture"}}
}

func TestMainlandProxyExactAndSuffixSemanticsAndRoundtrip(t *testing.T) {
	profile := mainlandTestProfile()
	if err := profile.validate(); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(profile)
	if err != nil {
		t.Fatal(err)
	}
	var decoded mainlandProxyProfile
	if err = json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	for host, want := range map[string]string{"github.com": "mainland_proxy_gfw_denied", "api.github.com": "mainland_proxy_gfw_denied", "raw.githubusercontent.com": "mainland_proxy_gfw_denied", "gitlab.com": "mainland_proxy_greatfire_denied", "sub.gitlab.com": "", "notgithub.com": "", "github.com.example.org": "", "pypi.org": "", "sub.exact.example.org": "", "exact.example.org": "mainland_proxy_gfw_denied"} {
		if got := decoded.denialReason(webDestination{Host: host, Protocol: "https"}); got != want {
			t.Errorf("%s: %q != %q", host, got, want)
		}
	}
	profile.GFWSuffixes = append(profile.GFWSuffixes, "github.com")
	if profile.validate() == nil {
		t.Fatal("duplicate/unsorted rules were accepted")
	}
}

func TestMainlandProxyRealHTTPConnectAndGuestUseSameRules(t *testing.T) {
	for _, guest := range []bool{false, true} {
		t.Run(map[bool]string{false: "container", true: "guest"}[guest], func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, "real-package-bytes") })
			plain := httptest.NewServer(handler)
			defer plain.Close()
			secure := httptest.NewTLSServer(handler)
			defer secure.Close()
			var dials atomic.Int64
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ws, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer ws.CloseNow()
				_, raw, err := ws.Read(r.Context())
				if err != nil {
					return
				}
				var destination webDestination
				if json.Unmarshal(raw, &destination) != nil {
					t.Error("invalid destination")
					return
				}
				dials.Add(1)
				target := plain.URL
				if destination.Protocol == "https" {
					target = secure.URL
				}
				remote, err := net.Dial("tcp", strings.TrimPrefix(strings.TrimPrefix(target, "http://"), "https://"))
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
			root, _ := url.Parse(gateway.URL + "/internal/service-execution")
			tokenFile := filepath.Join(t.TempDir(), "pod-token")
			if err := os.WriteFile(tokenFile, []byte("fixture-pod-token"), 0600); err != nil {
				t.Fatal(err)
			}
			broker := &workloadBroker{podTokenFile: tokenFile, root: root, identity: workloadIdentity{LeaseID: "fixture-lease", Generation: 1, ExecutionRole: "attempt"}, client: gateway.Client()}
			broker.setPhase("agent", time.Now().Add(time.Minute))
			policy := phasedEgressPolicy{broker: broker, task: &publicWeb{Kind: "public-web"}, mainland: mainlandTestProfile()}
			var evidence bytes.Buffer
			start := broker.startTaskEgress
			if guest {
				start = broker.startGuestTaskEgress
			}
			proxy, stop, err := start(context.Background(), policy, "sha256:fixture", &evidence)
			if err != nil {
				t.Fatal(err)
			}
			defer stop()
			if !strings.Contains(evidence.String(), `"outcome":"mainland_proxy_loaded"`) || !strings.Contains(evidence.String(), `"gfw_suffixes":2`) {
				t.Fatal("startup did not record the validated profile and loaded rule count")
			}
			proxyURL, _ := url.Parse(proxy)
			transport := &http.Transport{Proxy: http.ProxyURL(proxyURL), TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} // local fixture certificate only
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
			for _, target := range []string{"http://pypi.org/simple/six/", "https://registry.npmjs.org/is-number"} {
				response, err := client.Get(target)
				if err != nil {
					t.Fatal(err)
				}
				body, _ := io.ReadAll(response.Body)
				response.Body.Close()
				if response.StatusCode != 200 || string(body) != "real-package-bytes" {
					t.Fatalf("allowed transfer failed %d %q", response.StatusCode, body)
				}
			}
			before := dials.Load()
			for _, item := range []struct{ host, reason string }{{"github.com", "mainland_proxy_gfw_denied"}, {"raw.githubusercontent.com", "mainland_proxy_gfw_denied"}, {"gitlab.com", "mainland_proxy_greatfire_denied"}} {
				response, err := client.Get("http://" + item.host + "/resource")
				if err != nil {
					t.Fatal(err)
				}
				response.Body.Close()
				if response.StatusCode != 403 || response.Header.Get("X-Loom-Egress-Error") != item.reason {
					t.Fatalf("HTTP restriction missing for %s", item.host)
				}
				request, _ := http.NewRequest("CONNECT", proxy, nil)
				request.Host = item.host + ":443"
				response, err = (&http.Client{Timeout: time.Second}).Do(request)
				if err != nil {
					t.Fatal(err)
				}
				response.Body.Close()
				if response.StatusCode != 403 || response.Header.Get("X-Loom-Egress-Error") != item.reason {
					t.Fatalf("CONNECT restriction missing for %s", item.host)
				}
			}
			if dials.Load() != before {
				t.Fatal("a blocked destination reached the upstream dialer")
			}
			broker.setPhase("verifier", time.Now().Add(time.Minute))
			if policy.permits(webDestination{Host: "gitlab.com", Protocol: "https"}) {
				t.Fatal("verifier escaped the fixed profile")
			}
			broker.setPhase("setup", time.Now().Add(time.Minute))
			policy.setup = &webAllowlist{Kind: "web-allowlist", Destinations: []webDestination{{Host: "github.com", Protocol: "https"}}}
			if !policy.permits(webDestination{Host: "github.com", Protocol: "https"}) {
				t.Fatal("task restrictions changed trusted setup egress")
			}
			if !strings.Contains(evidence.String(), "mainland_proxy_gfw_denied") || !strings.Contains(evidence.String(), "mainland_proxy_greatfire_denied") {
				t.Fatal("restriction evidence missing")
			}
		})
	}
}

func TestMainlandProxyCannotRemovePublicWebEgress(t *testing.T) {
	p := plan{MainlandProxyProfile: mainlandTestProfile(), EffectiveNetworkPolicy: json.RawMessage(`{"kind":"public-web"}`)}
	p.Main.Environment = map[string]string{effectiveNetworkPolicyEnv: `{"kind":"public-web"}`}
	if err := p.validateEffectiveNetworkPolicy(); err == nil {
		t.Fatal("public-web profile without the task dialer was accepted")
	}
	// A strictly closed verifier does not need a web proxy. Retaining its
	// frozen profile does not authorize a connection or claim it was loaded.
	p.EffectiveNetworkPolicy = json.RawMessage(`{"kind":"gateway-only"}`)
	p.Main.Environment = map[string]string{effectiveNetworkPolicyEnv: `{"kind":"gateway-only"}`}
	if err := p.validateEffectiveNetworkPolicy(); err != nil {
		t.Fatal(err)
	}
}
