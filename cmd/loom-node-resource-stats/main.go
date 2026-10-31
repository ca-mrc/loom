// loom-node-resource-stats reads a typed request and returns numeric observations.
// It has no listener and exposes no general container-runtime RPC forwarding.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/grpclog"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const maxPayload = 700 * 1024
const observationBudget = 10 * time.Second

type options struct {
	runtimeEndpoint, podLogsRoot, kubeletPodsRoot string
}

type outcome struct {
	body []byte
	code string
}

func main() {
	// Upstream transport errors can contain host paths. Only our finite codes
	// are observable, including when deployment flags are invalid.
	grpclog.SetLoggerV2(grpclog.NewLoggerV2(io.Discard, io.Discard, io.Discard))
	flags := flag.NewFlagSet("loom-node-resource-stats", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	opts := options{}
	flags.StringVar(&opts.runtimeEndpoint, "runtime-endpoint", "unix:///run/containerd/containerd.sock", "")
	flags.StringVar(&opts.podLogsRoot, "pod-logs-root", "/host/pod-logs", "")
	flags.StringVar(&opts.kubeletPodsRoot, "kubelet-pods-root", "/host/kubelet-pods", "")
	if flags.Parse(os.Args[1:]) != nil || flags.NArg() != 0 {
		_, _ = io.WriteString(os.Stdout, "{\"error\":\"invalid_arguments\"}\n")
		os.Exit(2)
	}
	os.Exit(run(context.Background(), opts, os.Stdin, os.Stdout))
}

func run(parent context.Context, opts options, input io.Reader, output io.Writer) int {
	ctx, cancel := context.WithTimeout(parent, observationBudget)
	defer cancel()
	finished := make(chan outcome, 1)
	go func() { finished <- observe(ctx, opts, input) }()
	var result outcome
	select {
	case result = <-finished:
	case <-ctx.Done():
		result.code = "observation_deadline"
	}
	return writeOutcome(output, result)
}

func writeOutcome(output io.Writer, result outcome) int {
	if result.code != "" {
		result.body, _ = json.Marshal(map[string]string{"error": result.code})
	}
	if len(result.body)+1 > maxPayload {
		result.body = []byte(`{"error":"response_too_large"}`)
		result.code = "response_too_large"
	}
	if _, err := output.Write(append(result.body, '\n')); err != nil {
		return 1
	}
	if result.code != "" {
		return 1
	}
	return 0
}

func observe(ctx context.Context, opts options, input io.Reader) outcome {
	req, code := decodeRequest(input)
	if code != "" {
		return outcome{code: code}
	}
	if ctx.Err() != nil {
		return outcome{code: "observation_deadline"}
	}
	endpoint, err := url.Parse(opts.runtimeEndpoint)
	if err != nil || endpoint.Scheme != "unix" || endpoint.Host != "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" || !filepath.IsAbs(endpoint.Path) || filepath.Clean(endpoint.Path) != endpoint.Path || len(endpoint.Path) > 103 || !filepath.IsAbs(opts.podLogsRoot) || !filepath.IsAbs(opts.kubeletPodsRoot) {
		return outcome{code: "invalid_configuration"}
	}
	if len(req.Pods) == 0 {
		return outcome{body: []byte(`{"pods":[]}`)}
	}
	client, err := grpc.NewClient("passthrough:///local-cri",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithNoProxy(),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", endpoint.Path)
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxPayload)),
	)
	if err != nil {
		return outcome{code: "runtime_unavailable"}
	}
	defer client.Close()
	summary := collect(ctx, req, runtime.NewRuntimeServiceClient(client), opts, time.Now)
	if ctx.Err() != nil {
		return outcome{code: "observation_deadline"}
	}
	var body bytes.Buffer
	if err := json.NewEncoder(&body).Encode(summary); err != nil {
		return outcome{code: "response_unavailable"}
	}
	result := bytes.TrimSuffix(body.Bytes(), []byte{'\n'})
	if len(result)+1 > maxPayload {
		return outcome{code: "response_too_large"}
	}
	return outcome{body: result}
}
