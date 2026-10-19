package main

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/qianyi-sun/loom/internal/guestchannel"
)

// Model transport backpressure without making Close itself uninterruptible.
type stalledGuestConn struct {
	net.Conn
	stalled              atomic.Bool
	writeStarted, closed chan struct{}
	writeOnce, closeOnce sync.Once
}

func (c *stalledGuestConn) Write(p []byte) (int, error) {
	if c.stalled.Load() {
		c.writeOnce.Do(func() { close(c.writeStarted) })
		<-c.closed
		return 0, net.ErrClosed
	}
	return c.Conn.Write(p)
}

func (c *stalledGuestConn) Close() error {
	c.closeOnce.Do(func() { close(c.closed); _ = c.Conn.Close() })
	return nil
}

type guestTransportFixture struct {
	channel                        *guestchannel.Client
	connection                     *stalledGuestConn
	transport                      *http.Transport
	client                         *http.Client
	activeStarted, activeCancelled chan struct{}
}

func awaitGuestCleanup(t *testing.T, done <-chan struct{}, operation string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("%s did not finish within 1s", operation)
	}
}

func newGuestTransportFixture(t *testing.T) *guestTransportFixture {
	t.Helper()
	host, guest := net.Pipe()
	connection := &stalledGuestConn{Conn: host, writeStarted: make(chan struct{}), closed: make(chan struct{})}
	type listenerResult struct {
		listener net.Listener
		err      error
	}
	ready := make(chan listenerResult, 1)
	go func() {
		listener, err := guestchannel.Serve(guest)
		ready <- listenerResult{listener, err}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	channel, err := guestchannel.Connect(ctx, connection)
	if err != nil {
		t.Fatal(err)
	}
	result := <-ready
	if result.err != nil {
		t.Fatal(result.err)
	}
	f := &guestTransportFixture{
		channel: channel, connection: connection,
		activeStarted: make(chan struct{}), activeCancelled: make(chan struct{}),
	}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/active" {
			close(f.activeStarted)
			<-r.Context().Done()
			close(f.activeCancelled)
			return
		}
		w.Header().Set("Content-Length", "2")
		_, _ = io.WriteString(w, "ok")
	})}
	go func() { _ = server.Serve(result.listener) }()
	f.transport = &http.Transport{DialContext: channel.DialContext, MaxConnsPerHost: 128, MaxIdleConnsPerHost: 16}
	f.client = &http.Client{Transport: f.transport}
	// Also release a deliberately blocked regression after its bounded assertion
	// fails, so a failing test does not have to wait for yamux's write timeout.
	t.Cleanup(func() {
		_ = channel.Close()
		f.transport.CloseIdleConnections()
		_ = server.Close()
	})
	for i := 0; i < 2; i++ {
		idle := make(chan struct{})
		var idleOnce sync.Once
		var reused bool
		requestCtx, requestCancel := context.WithTimeout(context.Background(), time.Second)
		trace := &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) { reused = info.Reused },
			PutIdleConn: func(err error) {
				if err == nil {
					idleOnce.Do(func() { close(idle) })
				}
			},
		}
		req, _ := http.NewRequestWithContext(httptrace.WithClientTrace(requestCtx, trace), "GET", "http://guest/health", nil)
		response, err := f.client.Do(req)
		if err != nil {
			requestCancel()
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil || string(body) != "ok" {
			requestCancel()
			t.Fatalf("HTTP warmup: body=%q err=%v", body, err)
		}
		awaitGuestCleanup(t, idle, "HTTP idle registration")
		requestCancel()
		if i == 1 && !reused {
			t.Fatal("HTTP warmup did not reuse the established guest stream")
		}
	}
	return f
}

func TestCloseGuestTransportInterruptsIdleWrite(t *testing.T) {
	f := newGuestTransportFixture(t)
	f.connection.stalled.Store(true)
	done := make(chan struct{})
	go func() {
		closeGuestTransport(f.channel, f.transport)
		// The earlier defer channel.Close still runs after this helper.
		_ = f.channel.Close()
		closeGuestTransport(f.channel, f.transport)
		close(done)
	}()
	awaitGuestCleanup(t, done, "guest transport cleanup under write backpressure")
}

func TestCloseGuestTransportReleasesCancelledPendingDial(t *testing.T) {
	f := newGuestTransportFixture(t)
	f.connection.stalled.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		conn, err := f.channel.DialContext(ctx, "tcp", "ignored")
		if conn != nil {
			_ = conn.Close()
		}
		result <- err
	}()
	awaitGuestCleanup(t, f.connection.writeStarted, "pending guest stream SYN")
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("pending dial: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pending dial did not return context cancellation")
	}
	done := make(chan struct{})
	go func() { closeGuestTransport(f.channel, f.transport); close(done) }()
	awaitGuestCleanup(t, done, "guest transport cleanup with pending dial")
}

func TestCloseGuestTransportReleasesCancelledActiveHTTP(t *testing.T) {
	f := newGuestTransportFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", "http://guest/active", nil)
	result := make(chan error, 1)
	go func() {
		response, err := f.client.Do(req)
		if response != nil {
			_ = response.Body.Close()
		}
		result <- err
	}()
	awaitGuestCleanup(t, f.activeStarted, "active guest HTTP handler")
	f.connection.stalled.Store(true)
	cancel()
	awaitGuestCleanup(t, f.connection.writeStarted, "active HTTP cancellation FIN")
	done := make(chan struct{})
	go func() { closeGuestTransport(f.channel, f.transport); close(done) }()
	awaitGuestCleanup(t, done, "guest transport cleanup with active HTTP")
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("active HTTP cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("active HTTP request remained blocked after transport cleanup")
	}
	awaitGuestCleanup(t, f.activeCancelled, "active guest HTTP handler cancellation")
}
