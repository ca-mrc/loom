//go:build linux

package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestDockerReadinessUsesPrivateHTTPWithoutCLI(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "docker.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil { t.Fatal(err) }
	var calls atomic.Int32
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/_ping" || r.Method != http.MethodHead {
			t.Errorf("unexpected readiness request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if calls.Add(1) == 1 { w.WriteHeader(http.StatusServiceUnavailable); return }
		w.WriteHeader(http.StatusOK)
	})}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })
	t.Setenv("PATH", t.TempDir()) // readiness must not fork the Docker CLI
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := waitDocker(ctx, make(chan error), socket); err != nil { t.Fatal(err) }
	if calls.Load() < 2 { t.Fatal("unhealthy daemon was accepted") }
}

func TestDockerReadinessRetainsExitAndDeadline(t *testing.T) {
	for _, mode := range []string{"exited", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			exited := make(chan error, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			if mode == "exited" { exited <- errors.New("fixture daemon failure") }
			err := waitDocker(ctx, exited, filepath.Join(t.TempDir(), "absent.sock"))
			if mode == "exited" {
				if err == nil || !strings.Contains(err.Error(), "fixture daemon failure") { t.Fatalf("missing daemon failure: %v", err) }
			} else if !errors.Is(err, context.DeadlineExceeded) { t.Fatalf("missing deadline: %v", err) }
		})
	}
}
