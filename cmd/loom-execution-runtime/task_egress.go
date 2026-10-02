package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

var taskEgressOutput = outputDeclaration{SourcePath: ".loom/task-egress.jsonl", RelativePath: "diagnostics/task-egress.jsonl", Kind: "diagnostic", Required: true}

type webDestination struct {
	Host     string `json:"host"`
	Protocol string `json:"protocol"`
}
type webAllowlist struct {
	Kind         string           `json:"kind"`
	Destinations []webDestination `json:"destinations"`
}

var hostnameLabel = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)
var hostnameTLD = regexp.MustCompile(`^[a-z]{2,63}$`)

func (d webDestination) valid() bool {
	labels := strings.Split(d.Host, ".")
	if len(d.Host) > 253 || len(labels) < 2 || !hostnameTLD.MatchString(labels[len(labels)-1]) || (d.Protocol != "http" && d.Protocol != "https") {
		return false
	}
	for _, label := range labels {
		if !hostnameLabel.MatchString(label) {
			return false
		}
	}
	switch labels[len(labels)-1] {
	case "local", "internal", "localhost", "test", "invalid":
		return false
	}
	return true
}
func (p *webAllowlist) validate() error {
	if p.Kind != "web-allowlist" || len(p.Destinations) < 1 || len(p.Destinations) > 64 {
		return fmt.Errorf("invalid task egress policy")
	}
	previous := ""
	for _, d := range p.Destinations {
		key := d.Host + "/" + d.Protocol
		if !d.valid() || key <= previous {
			return fmt.Errorf("task egress destinations must be canonical, unique and sorted")
		}
		previous = key
	}
	return nil
}
func (p *webAllowlist) permits(d webDestination) bool {
	for _, item := range p.Destinations {
		if item == d {
			return true
		}
	}
	return false
}

type publicWeb struct {
	Kind string `json:"kind"`
}

func (p *publicWeb) validate() error {
	if p == nil || p.Kind != "public-web" {
		return fmt.Errorf("invalid task egress policy")
	}
	return nil
}

func (p *publicWeb) permits(webDestination) bool { return true }

type taskEgressPolicy interface {
	validate() error
	permits(webDestination) bool
}

type storedTaskEgress struct {
	taskEgressPolicy
}

// phasedEgressPolicy is the only policy the task-egress proxy enforces. A
// setup phase may reach only the declared install sources; every other phase
// gets exactly the task's frozen policy, which is none for gateway-only tasks.
type phasedEgressPolicy struct {
	broker *workloadBroker
	task   taskEgressPolicy
	setup  *webAllowlist
}

func (p phasedEgressPolicy) validate() error {
	if p.task == nil && p.setup == nil {
		return fmt.Errorf("task egress requires a task or setup policy")
	}
	if p.task != nil {
		if err := p.task.validate(); err != nil {
			return err
		}
	}
	if p.setup != nil {
		return p.setup.validate()
	}
	return nil
}

func (p phasedEgressPolicy) permits(d webDestination) bool {
	if p.broker.currentPhaseRole() == "setup" {
		return p.setup != nil && p.setup.permits(d)
	}
	return p.task != nil && p.task.permits(d)
}

func (s *storedTaskEgress) UnmarshalJSON(data []byte) error {
	var probe struct {
		Kind string `json:"kind"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return err
	}
	switch probe.Kind {
	case "web-allowlist":
		var policy webAllowlist
		if err := json.Unmarshal(data, &policy); err != nil {
			return err
		}
		s.taskEgressPolicy = &policy
	case "public-web":
		policy := publicWeb{Kind: "public-web"}
		s.taskEgressPolicy = &policy
	default:
		return fmt.Errorf("invalid task egress policy")
	}
	return nil
}

func (s storedTaskEgress) MarshalJSON() ([]byte, error) {
	if s.taskEgressPolicy == nil {
		return []byte("null"), nil
	}
	return json.Marshal(s.taskEgressPolicy)
}

const effectiveNetworkPolicyEnv = "LOOM_EFFECTIVE_NETWORK_POLICY_JSON"

var networkPolicyKinds = map[string]bool{
	"public": true, "no-network": true, "gateway-only": true,
	"allowlist": true, "web-allowlist": true, "public-web": true,
}

func decodeNetworkPolicy(data []byte) (map[string]any, error) {
	var policy map[string]any
	if err := json.Unmarshal(data, &policy); err != nil || policy == nil {
		return nil, fmt.Errorf("effective network policy is invalid")
	}
	return policy, nil
}

// The control plane freezes one effective policy into the plan, the task
// egress dialer and every phase environment; all three must agree.
func (p plan) validateEffectiveNetworkPolicy() error {
	phases := []phase{p.Main}
	if p.Verifier != nil {
		phases = append(phases, *p.Verifier)
	}
	if len(p.EffectiveNetworkPolicy) == 0 {
		for _, item := range phases {
			if _, ok := item.Environment[effectiveNetworkPolicyEnv]; ok {
				return fmt.Errorf("phase network policy has no immutable plan authority")
			}
		}
		return nil
	}
	policy, err := decodeNetworkPolicy(p.EffectiveNetworkPolicy)
	if err != nil {
		return err
	}
	kind, _ := policy["kind"].(string)
	if !networkPolicyKinds[kind] {
		return fmt.Errorf("effective network policy is invalid")
	}
	if kind == "web-allowlist" || kind == "public-web" {
		if p.TaskEgress == nil || p.TaskEgress.taskEgressPolicy == nil {
			return fmt.Errorf("task egress does not match the effective network policy")
		}
		encoded, err := json.Marshal(p.TaskEgress)
		if err != nil {
			return err
		}
		egress, err := decodeNetworkPolicy(encoded)
		if err != nil || !reflect.DeepEqual(policy, egress) {
			return fmt.Errorf("task egress does not match the effective network policy")
		}
	} else if p.TaskEgress != nil {
		return fmt.Errorf("task egress does not match the effective network policy")
	}
	for _, item := range phases {
		raw, ok := item.Environment[effectiveNetworkPolicyEnv]
		if !ok {
			return fmt.Errorf("phase is missing the effective network policy")
		}
		phasePolicy, err := decodeNetworkPolicy([]byte(raw))
		if err != nil {
			return fmt.Errorf("phase effective network policy is invalid")
		}
		if !reflect.DeepEqual(policy, phasePolicy) {
			return fmt.Errorf("phase effective network policy differs from the plan")
		}
	}
	return nil
}
func requestDestination(r *http.Request) (webDestination, error) {
	if r.Method == http.MethodConnect {
		host, port, err := net.SplitHostPort(r.Host)
		d := webDestination{host, "https"}
		if err != nil || port != "443" || !d.valid() || r.URL.RawQuery != "" || r.URL.User != nil {
			return d, fmt.Errorf("task_egress_request_invalid")
		}
		return d, nil
	}
	d := webDestination{r.URL.Hostname(), "http"}
	if r.URL.Scheme != "http" || !d.valid() || (r.URL.Port() != "" && r.URL.Port() != "80") || r.URL.User != nil || r.URL.Fragment != "" || r.Host != r.URL.Host || r.Header.Get("Upgrade") != "" {
		return d, fmt.Errorf("task_egress_request_invalid")
	}
	return d, nil
}

type taskEgressAudit struct {
	mu           sync.Mutex
	output       io.Writer
	entries      int
	bytes, limit int64
	truncated    bool
}

func (a *taskEgressAudit) record(d webDestination, outcome string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.truncated {
		return
	}
	if !d.valid() {
		d = webDestination{}
	}
	payload, _ := json.Marshal(map[string]any{"time": time.Now().UTC(), "host": d.Host, "protocol": d.Protocol, "outcome": outcome})
	payload = append(payload, '\n')
	marker := []byte("{\"outcome\":\"task_egress_diagnostics_truncated\"}\n")
	if a.entries >= 4096 || a.bytes+int64(len(payload)+len(marker)) > a.limit {
		a.truncated = true
		if a.bytes+int64(len(marker)) <= a.limit {
			_, _ = a.output.Write(marker)
		}
		return
	}
	a.entries++
	a.bytes += int64(len(payload))
	_, _ = a.output.Write(payload)
}

func (b *workloadBroker) taskTunnel(ctx context.Context, d webDestination, digest string, deadline time.Time, role string) (net.Conn, error) {
	endpoint := b.endpoint("/task-egress")
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	b.identityHeaders(request)
	if err := b.authorizePodRequest(request); err != nil {
		return nil, fmt.Errorf("task_egress_identity_unavailable")
	}
	request.Header.Set("X-Loom-Runtime-Contract-SHA256", digest)
	request.Header.Set("X-Loom-Phase-Deadline", deadline.UTC().Format(time.RFC3339Nano))
	// The Gateway admits setup-only install sources only for setup phases.
	request.Header.Set("X-Loom-Execution-Phase", role)
	client := *b.client
	client.Timeout = 0
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	ws, _, err := websocket.Dial(dialCtx, endpoint, &websocket.DialOptions{HTTPClient: &client, HTTPHeader: request.Header})
	if err != nil {
		return nil, fmt.Errorf("task_egress_gateway_unavailable")
	}
	ws.SetReadLimit(65536)
	raw, _ := json.Marshal(d)
	if err = ws.Write(dialCtx, websocket.MessageText, raw); err != nil {
		ws.CloseNow()
		return nil, fmt.Errorf("task_egress_gateway_unavailable")
	}
	kind, raw, err := ws.Read(dialCtx)
	var ready struct {
		Status string `json:"status"`
	}
	if err != nil || kind != websocket.MessageText || json.Unmarshal(raw, &ready) != nil || ready.Status != "ready" {
		ws.CloseNow()
		var closed websocket.CloseError
		if errors.As(err, &closed) {
			switch closed.Reason {
			case "destination_dns_failed", "destination_address_denied", "destination_connect_failed", "destination_connect_timeout", "task_egress_destination_denied", "task_egress_not_declared", "task_egress_identity_rejected", "task_egress_unavailable", "task_egress_capacity_exceeded", "task_egress_deadline", "task_egress_contract_mismatch":
				return nil, errors.New(closed.Reason)
			}
		}
		return nil, fmt.Errorf("task_egress_gateway_unavailable")
	}
	return websocket.NetConn(ctx, ws, websocket.MessageBinary), nil
}

func (b *workloadBroker) startTaskEgress(parent context.Context, policy taskEgressPolicy, digest string, evidence io.Writer, evidenceLimits ...int64) (string, func() error, error) {
	return b.startTaskEgressAt(parent, policy, digest, evidence, "127.0.0.1:0", evidenceLimits...)
}

// The guest Docker daemon starts before the controller phase. A stable,
// Pod-local endpoint lets it use the same phase-fenced allowlist without a
// restart, credentials, or an additional listener outside the Pod.
func (b *workloadBroker) startGuestTaskEgress(parent context.Context, policy taskEgressPolicy, digest string, evidence io.Writer, evidenceLimits ...int64) (string, func() error, error) {
	return b.startTaskEgressAt(parent, policy, digest, evidence, "127.0.0.1:18791", evidenceLimits...)
}

func (b *workloadBroker) startTaskEgressAt(parent context.Context, policy taskEgressPolicy, digest string, evidence io.Writer, address string, evidenceLimits ...int64) (string, func() error, error) {
	if err := policy.validate(); err != nil {
		return "", nil, err
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return "", nil, err
	}
	lifetime, stop := context.WithCancel(parent)
	// Package clients retain metadata and redirect sockets alongside downloads.
	// Match Gateway's per-lease ceiling; its configured and global caps still apply.
	slots := make(chan struct{}, 32)
	audit := taskEgressAudit{output: evidence, limit: 1024 * 1024}
	for _, limit := range evidenceLimits {
		if limit < audit.limit {
			audit.limit = limit
		}
	}
	server := &http.Server{ReadHeaderTimeout: 10 * time.Second, MaxHeaderBytes: 32768}
	var gate sync.Mutex
	var running sync.WaitGroup
	closed := false
	server.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gate.Lock()
		if closed {
			gate.Unlock()
			http.Error(w, "task_egress_stopped", 503)
			return
		}
		running.Add(1)
		gate.Unlock()
		defer running.Done()
		d, err := requestDestination(r)
		outcome := "task_egress_completed"
		defer func() { audit.record(d, outcome) }()
		fail := func(reason string, status int) {
			outcome = reason
			w.Header().Set("X-Loom-Egress-Error", reason)
			http.Error(w, reason, status)
		}
		if err != nil {
			fail("task_egress_request_invalid", 400)
			return
		}
		if !policy.permits(d) {
			fail("task_egress_destination_denied", 403)
			return
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			fail("task_egress_capacity_exceeded", 429)
			return
		}
		b.mu.Lock()
		deadline, phaseContext, role := b.phaseDeadline, b.phaseContext, b.phaseRole
		b.mu.Unlock()
		if phaseContext == nil || !deadline.After(time.Now()) || lifetime.Err() != nil {
			fail("task_egress_deadline", 504)
			return
		}
		ctx, cancel := context.WithDeadline(r.Context(), deadline)
		defer cancel()
		stopParent := context.AfterFunc(lifetime, cancel)
		defer stopParent()
		stopPhase := context.AfterFunc(phaseContext, cancel)
		defer stopPhase()
		tunnel, err := b.taskTunnel(ctx, d, digest, deadline, role)
		if err != nil {
			fail(err.Error(), 502)
			return
		}
		defer tunnel.Close()
		if r.Method == http.MethodConnect {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				fail("task_egress_proxy_unavailable", 500)
				return
			}
			downstream, buffer, err := hijacker.Hijack()
			if err != nil {
				outcome = "task_egress_transport_failed"
				return
			}
			defer downstream.Close()
			stopDownstream := context.AfterFunc(ctx, func() { _ = downstream.Close() })
			defer stopDownstream()
			_, _ = buffer.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n")
			if buffer.Flush() != nil {
				return
			}
			done := make(chan error, 2)
			go func() { _, err := io.Copy(tunnel, buffer); done <- err }()
			go func() { _, err := io.Copy(downstream, tunnel); done <- err }()
			first := <-done
			_ = downstream.Close()
			_ = tunnel.Close()
			<-done
			if first != nil {
				outcome = "task_egress_transport_failed"
			}
		} else {
			// A fresh bound connection handles one ordinary HTTP request. Redirects are
			// returned to the task; its next request must pass the allowlist again.
			request := r.Clone(ctx)
			request.RequestURI = ""
			request.Close = true
			for _, name := range strings.Split(request.Header.Get("Connection"), ",") {
				request.Header.Del(strings.TrimSpace(name))
			}
			for _, name := range []string{"Proxy-Authorization", "Proxy-Connection", "Connection", "Keep-Alive", "TE", "Trailer", "Transfer-Encoding", "Upgrade"} {
				request.Header.Del(name)
			}
			request.Header.Set("Connection", "close")
			transport := &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) { return tunnel, nil }, DisableKeepAlives: true, MaxResponseHeaderBytes: 32768, ResponseHeaderTimeout: 30 * time.Second}
			defer transport.CloseIdleConnections()
			response, err := transport.RoundTrip(request)
			if err != nil {
				fail("task_egress_transport_failed", 502)
				return
			}
			defer response.Body.Close()
			for name, values := range response.Header {
				for _, value := range values {
					w.Header().Add(name, value)
				}
			}
			w.Header().Del("Connection")
			w.Header().Del("Transfer-Encoding")
			w.WriteHeader(response.StatusCode)
			if _, err = io.Copy(w, response.Body); err != nil {
				outcome = "task_egress_transport_failed"
			}
		}
	})
	go func() { _ = server.Serve(listener) }()
	return "http://" + listener.Addr().String(), func() error {
		gate.Lock()
		closed = true
		gate.Unlock()
		stop()
		err := server.Close()
		running.Wait()
		return err
	}, nil
}
