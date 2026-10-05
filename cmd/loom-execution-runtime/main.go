// Command loom-execution-runtime is PID 1 inside one Kubernetes attempt container.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "materialize" {
		if err := materialize(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		return
	}
	planPath := flag.String("plan", "/etc/loom/execution-plan.json", "immutable execution plan")
	workspace := flag.String("workspace", "/workspace", "bounded workspace volume")
	outputRoot := flag.String("output-root", "/loom/output", "bounded evidence volume")
	absoluteDeadline := flag.String("deadline-at", "", "original absolute workload deadline (RFC3339)")
	terminationMessage := flag.String(
		"termination-message",
		"/loom/output/termination-message",
		"bounded Kubernetes termination summary",
	)
	flag.Parse()

	p, err := loadPlan(*planPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	signalContext, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stopSignals()
	ctx, cancel, err := workloadContext(signalContext, *absoluteDeadline)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	defer cancel()
	cleanOutput := filepath.Clean(*outputRoot)
	cleanTerminationMessage := filepath.Clean(*terminationMessage)
	if !isWithin(cleanOutput, cleanTerminationMessage) {
		fmt.Fprintln(os.Stderr, "termination message must be inside output root")
		os.Exit(2)
	}
	broker, err := workloadBrokerFromEnvironment()
	if err != nil {
		fmt.Fprintln(os.Stderr, "initialize workload broker:", err)
		os.Exit(2)
	}
	if err := hardenRuntimeIdentity(); err != nil {
		fmt.Fprintln(os.Stderr, "harden workload broker identity:", err)
		os.Exit(2)
	}
	if p.TaskInput != nil {
		inputContext, stopInput := context.WithTimeout(ctx, 10*time.Minute)
		err = broker.materializeInputs(inputContext, p, filepath.Clean(*workspace))
		if err == nil {
			err = broker.materializeHandoff(inputContext, p, filepath.Clean(*workspace))
		}
		stopInput()
		if err != nil {
			fmt.Fprintln(os.Stderr, "materialize task input:", err)
			os.Exit(2)
		}
	}
	runSetupCacheStep(ctx, "fetch", func(ctx context.Context) error {
		return broker.fetchSetupCache(ctx, p, filepath.Clean(*workspace))
	})
	executionContext, stopMonitor := monitorPrivateSandboxes(ctx, p)
	defer stopMonitor()
	broker.setPhase("", time.Time{})
	proxyURL, stopProxy, err := broker.startProxy(ctx, executionContext)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start workload proxy:", err)
		os.Exit(2)
	}
	defer func() { _ = stopProxy() }()
	trustedEnvironment := trustedGatewayEnvironment(proxyURL)
	for _, sidecar := range p.Sidecars {
		if sidecar.GuestExecution != nil {
			trustedEnvironment["LOOM_SANDBOX_MAX_TRANSFER_BYTES"] = strconv.FormatInt(p.MaxArtifactBytes, 10)
			break
		}
	}
	stopTaskEgress := func() {}
	if p.TaskEgress != nil || p.SetupEgress != nil {
		diagnosticDirectory := filepath.Join(filepath.Clean(*workspace), filepath.Dir(taskEgressOutput.SourcePath))
		if err := secureInputDirectory(filepath.Clean(*workspace), diagnosticDirectory); err != nil {
			fmt.Fprintln(os.Stderr, "prepare task egress evidence:", err)
			os.Exit(2)
		}
		evidence, err := os.OpenFile(filepath.Join(diagnosticDirectory, filepath.Base(taskEgressOutput.SourcePath)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			fmt.Fprintln(os.Stderr, "prepare task egress evidence:", err)
			os.Exit(2)
		}
		defer evidence.Close()
		startEgress := broker.startTaskEgress
		for _, sidecar := range p.Sidecars {
			if sidecar.GuestExecution != nil {
				startEgress = broker.startGuestTaskEgress
				break
			}
		}
		policy := phasedEgressPolicy{broker: broker, setup: p.SetupEgress}
		if p.TaskEgress != nil {
			policy.task = p.TaskEgress.taskEgressPolicy
		}
		proxy, stop, err := startEgress(executionContext, policy, p.RuntimeContractSHA256, evidence, p.MaxArtifactBytes, p.MaxLogBytesPerStream)
		if err != nil {
			fmt.Fprintln(os.Stderr, "start task egress:", err)
			os.Exit(2)
		}
		defer stop()
		stopTaskEgress = func() { _ = stop(); _ = evidence.Close() }
		trustedEnvironment["LOOM_TASK_EGRESS_PROXY"] = proxy
		for _, name := range []string{"http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"} {
			trustedEnvironment[name] = proxy
		}
		trustedEnvironment["no_proxy"] = "localhost,127.0.0.1,::1"
		trustedEnvironment["NO_PROXY"] = "localhost,127.0.0.1,::1"
	}
	result, runErr := runPlan(
		executionContext,
		p,
		filepath.Clean(*workspace),
		cleanOutput,
		trustedEnvironment,
		broker.setPhase,
	)
	stopTaskEgress()
	stopMonitor()
	runSetupCacheStep(ctx, "store", func(ctx context.Context) error {
		return broker.storeSetupCache(ctx, p, filepath.Clean(*workspace))
	})
	captureErr := captureDeclaredOutputs(p, filepath.Clean(*workspace), cleanOutput, &result)
	if captureErr != nil {
		result.FinishedAt = time.Now().UTC()
		fmt.Fprintln(os.Stderr, "capture complete trial bundle:", captureErr)
	}
	if err := writeResult(filepath.Join(cleanOutput, "result.json"), result); err != nil {
		fmt.Fprintln(os.Stderr, "write result:", err)
		os.Exit(3)
	}
	commitContext, stopCommit := context.WithTimeout(context.Background(), 5*time.Minute)
	evidence, commitErr := broker.commitOutputs(commitContext, cleanOutput, broker.outputRequestID())
	stopCommit()
	if commitErr != nil {
		// A successful command without durable output is a runtime failure. Rewrite
		// the local semantic result before publishing the bounded termination summary.
		if result.Status == "succeeded" {
			result.Status = "runtime_error"
			result.PartialEvidence = true
			result.FinishedAt = time.Now().UTC()
			if err := writeResult(filepath.Join(cleanOutput, "result.json"), result); err != nil {
				fmt.Fprintln(os.Stderr, "rewrite failed output result:", err)
				os.Exit(3)
			}
		}
		fmt.Fprintln(os.Stderr, "commit runtime output:", commitErr)
	}
	var committed *outputCommitEvidence
	if commitErr == nil {
		committed = &evidence
	}
	if err := writeTerminationSummary(cleanTerminationMessage, result, committed); err != nil {
		fmt.Fprintln(os.Stderr, "write termination summary:", err)
		os.Exit(3)
	}
	if commitErr != nil {
		os.Exit(3)
	}
	if runErr != nil {
		fmt.Fprintln(os.Stderr, runErr)
		os.Exit(1)
	}
	if captureErr != nil {
		os.Exit(1)
	}
}
