package main

import (
	"context"
	"fmt"
	"time"
)

// workloadContext bounds input/proxy/phase work by the original lease, not by
// container startup time. Output commit deliberately retains its separate
// bounded context so deadline cancellation can still publish partial evidence.
func workloadContext(parent context.Context, absoluteDeadline string) (context.Context, context.CancelFunc, error) {
	if absoluteDeadline == "" {
		ctx, cancel := context.WithCancel(parent)
		return ctx, cancel, nil // Compatibility for pre-migration runtime invocations.
	}
	deadline, err := time.Parse(time.RFC3339Nano, absoluteDeadline)
	if err != nil {
		return nil, nil, fmt.Errorf("invalid absolute workload deadline")
	}
	if !time.Now().Before(deadline) {
		return nil, nil, fmt.Errorf("workload deadline elapsed")
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	return ctx, cancel, nil
}
