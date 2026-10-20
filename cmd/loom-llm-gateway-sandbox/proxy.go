// HTTP CONNECT proxy handler. Validates the step-JWT, dials the
// configured upstream, hijacks the client connection, and copies
// bytes bidirectionally until either side closes.
//
// We do NOT proxy regular HTTP verbs (GET/POST/etc) — the sandbox
// reaches LLM APIs over HTTPS, which always uses CONNECT. Anything
// non-CONNECT returns 405 to make the misuse obvious in logs.

package main

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

const (
	// Maximum time to establish the upstream TCP connection after a
	// CONNECT request is authenticated. Initial request headers have a
	// separate ReadHeaderTimeout on the http.Server in main.
	connectDialTimeout = 10 * time.Second
)

type connectProxy struct {
	upstreamHost string
	upstreamPort string
	signingKey   []byte
	logger       *slog.Logger
}

type connectProxyConfig struct {
	Upstream   string // e.g. http://gateway-router:30443
	SigningKey []byte
	Logger     *slog.Logger
}

func newConnectProxy(c connectProxyConfig) *connectProxy {
	u, err := url.Parse(c.Upstream)
	if err != nil {
		// Trip loud at startup; the operator can fix the flag.
		panic("upstream URL parse: " + err.Error())
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		// No port → default 30443 (the gateway-router port).
		host = u.Host
		port = "30443"
	}
	return &connectProxy{
		upstreamHost: host,
		upstreamPort: port,
		signingKey:   c.SigningKey,
		logger:       c.Logger,
	}
}

func (p *connectProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodConnect {
		http.Error(w, "only CONNECT is supported", http.StatusMethodNotAllowed)
		return
	}

	// Auth: extract the bearer from either Authorization or
	// x-api-key, verify the step-JWT signature + expiry.
	token := extractBearerToken(
		r.Header.Get("Authorization"),
		r.Header.Get("x-api-key"),
	)
	if token == "" {
		http.Error(w, "missing token", http.StatusUnauthorized)
		return
	}
	claims, err := verifyStepJWT(token, p.signingKey)
	if err != nil {
		// Don't log the token itself — extractBearerToken returns
		// raw bytes that an attacker could later replay.
		p.logger.Warn("auth_reject",
			"target", r.Host,
			"remote", r.RemoteAddr,
			"err", err)
		http.Error(w, "invalid token", http.StatusUnauthorized)
		return
	}

	upstreamAddr := net.JoinHostPort(p.upstreamHost, p.upstreamPort)
	p.logger.Info("connect_accept",
		"target", r.Host,
		"upstream", upstreamAddr,
		"team_id", claims.TeamID,
		"trial_id", claims.TrialID,
		"step_id", claims.StepID,
	)

	upstream, err := net.DialTimeout("tcp", upstreamAddr, connectDialTimeout)
	if err != nil {
		p.logger.Error("dial_upstream",
			"upstream", upstreamAddr, "err", err)
		http.Error(w, "bad gateway", http.StatusBadGateway)
		return
	}

	// Hijack the client connection to get the raw TCP bytes.
	// After a 200 response, both sides exchange application bytes
	// (the TLS handshake the sandbox does to its CONNECT target,
	// which we don't terminate).
	hj, ok := w.(http.Hijacker)
	if !ok {
		// Should never happen with the stdlib server, but if a
		// middleware wraps us in something non-hijacker we want
		// to fail loudly.
		upstream.Close()
		http.Error(w, "hijack unsupported", http.StatusInternalServerError)
		return
	}
	clientConn, _, err := hj.Hijack()
	if err != nil {
		upstream.Close()
		p.logger.Error("hijack", "err", err)
		http.Error(w, "hijack failed", http.StatusInternalServerError)
		return
	}
	defer clientConn.Close()

	// 200 Connection Established tells the client to start sending
	// raw TCP. Plain text — we're past the HTTP layer now.
	if _, err := clientConn.Write([]byte(
		"HTTP/1.1 200 Connection Established\r\n\r\n",
	)); err != nil {
		upstream.Close()
		p.logger.Error("write_connect_ok", "err", err)
		return
	}

	copyBidirectional(clientConn, upstream, p.logger)
}

// copyBidirectional pipes bytes both directions until either side
// closes. Mirrors httputil.TimeoutHandler-style semantics: one
// closed side cancels the other to avoid leaking goroutines.
//
// Errors are logged at Debug — the most common "errors" are
// io.EOF on graceful close, which is noise. Real failures (mid-
// stream reset, connection reset by peer) get Warn.
func copyBidirectional(client, upstream net.Conn, logger *slog.Logger) {
	defer upstream.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	pipe := func(dst, src net.Conn, dir string) {
		defer wg.Done()
		_, err := io.Copy(dst, src)
		// Close the OTHER side to unblock the paired io.Copy.
		// Close() is idempotent so the deferred Close above is
		// safe regardless of order.
		if c, ok := dst.(*net.TCPConn); ok {
			_ = c.CloseWrite()
		} else {
			_ = dst.Close()
		}
		if err != nil && !errors.Is(err, io.EOF) &&
			!errors.Is(err, net.ErrClosed) {
			logger.Warn("tunnel_io_error", "dir", dir, "err", err)
		}
	}

	go pipe(upstream, client, "client_to_upstream")
	pipe(client, upstream, "upstream_to_client")
	wg.Wait()
}
