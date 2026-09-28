//go:build linux

// loom-guest-runtime owns one disposable software VM inside a native sandbox.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if len(os.Args) == 1 && os.Args[0] == "/init" {
		guestInit()
		return
	}
	if len(os.Args) == 2 && os.Args[1] == "--sandbox-init" {
		sandboxInit()
		return
	}
	var c guestConfig
	flag.StringVar(&c.Payload, "payload", "/loom/runtime/guest", "immutable guest payload")
	flag.StringVar(&c.Root, "root", "/", "read-only task-image root")
	flag.StringVar(&c.State, "state", "", "fresh private guest state directory")
	flag.StringVar(&c.Socket, "socket", "", "sandbox API Unix socket")
	flag.IntVar(&c.MemoryMiB, "memory-mib", 0, "complete sandbox memory allocation")
	flag.IntVar(&c.StorageMiB, "storage-mib", 0, "maximum guest state disk size")
	flag.IntVar(&c.CPUMillis, "cpu-millis", 0, "complete sandbox CPU allocation")
	flag.Int64Var(&c.MaxTransfer, "max-transfer-bytes", 256<<20, "maximum sandbox transfer")
	flag.IntVar(&c.ExecTimeout, "exec-timeout-seconds", 900, "maximum exec deadline")
	flag.BoolVar(&c.NestedDocker, "nested-docker", false, "start a guest-owned Docker daemon")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := runGuest(ctx, c); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
