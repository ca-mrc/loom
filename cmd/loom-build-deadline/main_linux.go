// Command loom-build-deadline is the trusted PID1 of a native build phase.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	deadline := flag.String("deadline-at", "", "original absolute build deadline (RFC3339)")
	install := flag.Bool("install-runtime", false, "install the trusted guard for the following build phase")
	flag.Parse()
	// Exiting PID1 is the container boundary that also retires children which
	// escaped their process group. Do not silently weaken that contract.
	if os.Getpid() != 1 {
		fmt.Fprintln(os.Stderr, "build deadline guard must be PID1")
		os.Exit(2)
	}
	// Rootless BuildKit shares our UID, but may not ptrace or rewrite this guard.
	_, _, errno := syscall.Syscall6(syscall.SYS_PRCTL, 4, 0, 0, 0, 0, 0) // PR_SET_DUMPABLE
	if errno != 0 {
		fmt.Fprintln(os.Stderr, "cannot harden build deadline guard")
		os.Exit(2)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	if *install {
		if err := installRuntime(); err != nil {
			fmt.Fprintln(os.Stderr, "cannot install build deadline guard:", err)
			os.Exit(2)
		}
	}
	os.Exit(supervise(ctx, *deadline, flag.Args(), 10*time.Second))
}

func installRuntime() error {
	path, err := os.Executable()
	if err != nil {
		return err
	}
	source, err := os.Open(path)
	if err != nil {
		return err
	}
	defer source.Close()
	// Separate from untrusted build output, and mounted read-only in BuildKit.
	output, err := os.OpenFile("/loom/deadline-runtime/loom-build-deadline", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0500)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(output, source)
	return errors.Join(copyErr, output.Close())
}

func supervise(parent context.Context, rawDeadline string, argv []string, grace time.Duration) int {
	deadline, err := time.Parse(time.RFC3339Nano, rawDeadline)
	if err != nil || !time.Now().Before(deadline) {
		fmt.Fprintln(os.Stderr, "invalid or elapsed build deadline")
		return 124
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	if ctx.Err() != nil {
		return stoppedCode(ctx)
	}
	if len(argv) == 0 {
		return 2
	}
	command := exec.Command(argv[0], argv[1:]...)
	command.Stdin, command.Stdout, command.Stderr = os.Stdin, os.Stdout, os.Stderr
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := command.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "cannot start build phase:", err)
		return 2
	}
	waited := make(chan error, 1)
	go func() { waited <- command.Wait() }()
	select {
	case err := <-waited:
		if ctx.Err() != nil {
			return stoppedCode(ctx)
		}
		if err == nil {
			return 0
		}
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			if status, ok := exit.Sys().(syscall.WaitStatus); ok && status.Signaled() {
				return 128 + int(status.Signal())
			}
			return exit.ExitCode()
		}
		return 2
	case <-ctx.Done():
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGTERM)
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case <-waited:
		case <-timer.C:
			_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
			// Never extend grace waiting for an uninterruptible child. PID1
			// exit asks the container runtime to reap the entire namespace.
		}
		return stoppedCode(ctx)
	}
}

func stoppedCode(ctx context.Context) int {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return 124
	}
	return 143
}
