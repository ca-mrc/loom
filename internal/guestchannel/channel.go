// Package guestchannel carries the existing sandbox HTTP API over a single
// private virtio-serial stream. It never opens a network listener or reconnects
// to a replacement guest.
package guestchannel

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"github.com/hashicorp/yamux"
)

const greeting = "LOOMRPC1"

func config() *yamux.Config {
	c := yamux.DefaultConfig()
	c.LogOutput = io.Discard
	c.AcceptBacklog = 128
	c.StreamOpenTimeout = 10 * time.Second
	c.ConnectionWriteTimeout = 10 * time.Second
	return c
}

// Serve announces that guest bootstrap is complete. Opening the character
// device alone does not establish readiness: QEMU may already accept a host
// connection while the guest kernel is still booting.
func Serve(conn io.ReadWriteCloser) (net.Listener, error) {
	n, err := io.WriteString(conn, greeting)
	if err != nil || n != len(greeting) {
		conn.Close()
		return nil, errors.New("guest channel greeting failed")
	}
	return yamux.Server(conn, config())
}

type Client struct{ session *yamux.Session }

// Connect owns conn, including on failure or cancellation. There is deliberately
// no reconnect: the native sandbox monitor owns incarnation-loss handling.
func Connect(ctx context.Context, conn io.ReadWriteCloser) (*Client, error) {
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	buffer := make([]byte, len(greeting))
	_, err := io.ReadFull(conn, buffer)
	cancelled := !stop()
	if cancelled || ctx.Err() != nil {
		conn.Close()
		return nil, ctx.Err()
	}
	if err != nil || string(buffer) != greeting {
		conn.Close()
		return nil, errors.New("invalid guest channel greeting")
	}
	session, err := yamux.Client(conn, config())
	if err != nil {
		conn.Close()
		return nil, err
	}
	return &Client{session}, nil
}

func (c *Client) Close() error          { return c.session.Close() }
func (c *Client) Done() <-chan struct{} { return c.session.CloseChan() }

// DialContext matches http.Transport. network/address cannot select any peer;
// all connections remain streams within the one already-owned guest.
func (c *Client) DialContext(ctx context.Context, _, _ string) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	type result struct {
		conn net.Conn
		err  error
	}
	ready := make(chan result)
	go func() {
		conn, err := c.session.Open()
		select {
		case ready <- result{conn, err}:
		case <-ctx.Done():
			if conn != nil {
				conn.Close()
			}
		}
	}()
	select {
	case value := <-ready:
		return value.conn, value.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
