package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
)

const podUID = "11111111-1111-4111-8111-111111111111"
const containerID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func labels() map[string]string {
	return map[string]string{"io.kubernetes.pod.uid": podUID, "io.kubernetes.pod.namespace": "execution", "io.kubernetes.pod.name": "job-pod", "io.kubernetes.container.name": "execution"}
}

type fakeRuntime struct {
	runtime.UnimplementedRuntimeServiceServer
	created time.Time
	started time.Time
	stamp   time.Time
	calls   []string
	mutex   sync.Mutex
	damage  string
	delay   time.Duration
}

func (f *fakeRuntime) ContainerStatus(ctx context.Context, in *runtime.ContainerStatusRequest) (*runtime.ContainerStatusResponse, error) {
	f.mutex.Lock()
	f.calls = append(f.calls, "status")
	f.mutex.Unlock()
	if f.damage == "status_not_found" {
		return nil, status.Error(codes.NotFound, "private removed init metadata")
	}
	if in.ContainerId != containerID || in.Verbose {
		return nil, errors.New("private RPC input")
	}
	if f.delay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(f.delay):
		}
	}
	created := f.created
	if created.IsZero() {
		created = f.started.Add(-2 * time.Second)
	}
	reply := &runtime.ContainerStatusResponse{Status: &runtime.ContainerStatus{Id: containerID, Metadata: &runtime.ContainerMetadata{Name: "execution"}, Labels: labels(), CreatedAt: created.UnixNano(), StartedAt: f.started.UnixNano(), State: runtime.ContainerState_CONTAINER_RUNNING, LogPath: "/var/log/pods/execution_job-pod_" + podUID + "/execution/0.log"}}
	if strings.HasPrefix(f.damage, "exited") {
		reply.Status.State = runtime.ContainerState_CONTAINER_EXITED
		reply.Status.FinishedAt = f.started.Add(time.Second).UnixNano()
	}
	switch f.damage {
	case "status_uid":
		reply.Status.Labels["io.kubernetes.pod.uid"] = "22222222-2222-4222-8222-222222222222"
	case "status_namespace":
		reply.Status.Labels["io.kubernetes.pod.namespace"] = "foreign"
	case "status_name":
		reply.Status.Metadata.Name = "foreign"
	case "started":
		reply.Status.StartedAt -= int64(2 * time.Second)
	case "created_zero":
		reply.Status.CreatedAt = 0
	case "created_after_started":
		reply.Status.CreatedAt = reply.Status.StartedAt + 1
	case "created_negative":
		reply.Status.CreatedAt = -1
	case "log_path":
		reply.Status.LogPath = "/private/foreign.log"
	}
	return reply, nil
}
func (f *fakeRuntime) ContainerStats(ctx context.Context, in *runtime.ContainerStatsRequest) (*runtime.ContainerStatsResponse, error) {
	f.mutex.Lock()
	f.calls = append(f.calls, "stats")
	f.mutex.Unlock()
	if in.ContainerId != containerID {
		return nil, errors.New("private RPC input")
	}
	if strings.HasPrefix(f.damage, "exited") {
		return nil, status.Error(codes.NotFound, "private exited task metrics")
	}
	return f.statsReply(), nil
}

func (f *fakeRuntime) statsReply() *runtime.ContainerStatsResponse {
	reply := &runtime.ContainerStatsResponse{Stats: &runtime.ContainerStats{Attributes: &runtime.ContainerAttributes{Id: containerID, Metadata: &runtime.ContainerMetadata{Name: "execution"}, Labels: labels()}, Cpu: &runtime.CpuUsage{Timestamp: f.stamp.UnixNano(), UsageCoreNanoSeconds: &runtime.UInt64Value{Value: 1234567}}, Memory: &runtime.MemoryUsage{Timestamp: f.stamp.UnixNano(), UsageBytes: &runtime.UInt64Value{Value: 4096}}, WritableLayer: &runtime.FilesystemUsage{Timestamp: f.stamp.UnixNano(), UsedBytes: &runtime.UInt64Value{Value: 8192}}}}
	switch strings.TrimPrefix(f.damage, "exited_") {
	case "stats_uid":
		reply.Stats.Attributes.Labels["io.kubernetes.pod.uid"] = "22222222-2222-4222-8222-222222222222"
	case "stats_id":
		reply.Stats.Attributes.Id = strings.Repeat("b", 64)
	case "rootfs_nil":
		reply.Stats.WritableLayer = nil
	case "rootfs_value_nil":
		reply.Stats.WritableLayer.UsedBytes = nil
	case "rootfs_zero_timestamp":
		reply.Stats.WritableLayer.Timestamp = 0
	case "memory_value_nil":
		reply.Stats.Memory.UsageBytes = nil
	case "cpu_value_nil":
		reply.Stats.Cpu.UsageCoreNanoSeconds = nil
	case "rate_zero":
		reply.Stats.Cpu.UsageNanoCores = &runtime.UInt64Value{}
	case "rate_positive":
		reply.Stats.Cpu.UsageNanoCores = &runtime.UInt64Value{Value: 1000}
	case "counter_overflow":
		reply.Stats.Cpu.UsageCoreNanoSeconds.Value = ^uint64(0)
		reply.Stats.Memory.UsageBytes.Value = ^uint64(0)
		reply.Stats.WritableLayer.UsedBytes.Value = ^uint64(0)
	case "oversize_stats":
		reply.Stats.Attributes.Annotations = map[string]string{"private": strings.Repeat("private", maxPayload)}
	case "valid_zero":
		reply.Stats.Cpu.UsageCoreNanoSeconds.Value = 0
		reply.Stats.Memory.UsageBytes.Value = 0
		reply.Stats.WritableLayer.UsedBytes.Value = 0
	}
	return reply
}

func startFake(t *testing.T, f *fakeRuntime) string {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "loom-cri-")
	if err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(root, "runtime.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod != "/runtime.v1.RuntimeService/ContainerStatus" && info.FullMethod != "/runtime.v1.RuntimeService/ContainerStats" && info.FullMethod != "/runtime.v1.RuntimeService/ListContainerStats" {
			t.Errorf("unexpected RPC %s", info.FullMethod)
		}
		return handler(ctx, req)
	}))
	runtime.RegisterRuntimeServiceServer(server, f)
	go server.Serve(listener)
	t.Cleanup(func() { server.Stop(); listener.Close(); os.RemoveAll(root) })
	return "unix://" + socket
}

func requestJSON(started time.Time) string {
	return `{"schema_version":"loom.node-resource-request.v1","namespace":"execution","pods":[{"uid":"` + podUID + `","name":"job-pod","containers":[{"name":"execution","container_id":"containerd://` + containerID + `","started_at":"` + started.UTC().Truncate(time.Second).Format(time.RFC3339) + `"}],"volumes":[]}]}`
}

func TestCollectVerifiedCRIStatsThroughActualUnixRPC(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "http://invalid.example.test:9")
	t.Setenv("HTTP_PROXY", "http://invalid.example.test:9")
	f := &fakeRuntime{started: time.Now().Add(-time.Minute), stamp: time.Now()}
	logs := t.TempDir()
	logDir := filepath.Join(logs, "execution_job-pod_"+podUID, "execution")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(logDir, "0.log"), []byte("actual log bytes"), 0644); err != nil {
		t.Fatal(err)
	}
	pods := t.TempDir()
	if err := os.MkdirAll(filepath.Join(pods, podUID), 0755); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if got := run(context.Background(), options{startFake(t, f), logs, pods}, strings.NewReader(requestJSON(f.started)), &output); got != 0 {
		t.Fatalf("run=%d: %s", got, output.String())
	}
	var summary struct {
		Pods []struct {
			Containers []struct {
				ID  string `json:"_container_id"`
				CPU struct {
					Value uint64 `json:"usageCoreNanoSeconds"`
				} `json:"cpu"`
			} `json:"containers"`
			Ephemeral map[string]any `json:"ephemeral-storage"`
		} `json:"pods"`
	}
	if err := json.Unmarshal(output.Bytes(), &summary); err != nil {
		t.Fatal(err)
	}
	if len(summary.Pods) != 1 || len(summary.Pods[0].Containers) != 1 || summary.Pods[0].Containers[0].CPU.Value != 1234567 || summary.Pods[0].Containers[0].ID != "containerd://"+containerID {
		t.Fatalf("wrong verified stats: %s", output.String())
	}
	if len(summary.Pods[0].Ephemeral) == 0 {
		t.Fatalf("missing Pod disk total: %s", output.String())
	}
	if strings.Contains(output.String(), logs) || strings.Contains(output.String(), "runtime.sock") {
		t.Fatalf("private path leaked: %s", output.String())
	}
	if strings.Join(f.calls, ",") != "status,stats" {
		t.Fatalf("unexpected calls: %v", f.calls)
	}
}

func fixture(t *testing.T) options {
	t.Helper()
	logs, pods := t.TempDir(), t.TempDir()
	directory := filepath.Join(logs, "execution_job-pod_"+podUID, "execution")
	if err := os.MkdirAll(directory, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, "0.log"), []byte("log bytes"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(pods, podUID), 0755); err != nil {
		t.Fatal(err)
	}
	return options{"unix:///tmp/nonexistent.sock", logs, pods}
}

func sample(t *testing.T, damage string, stamp time.Time) (summary, *fakeRuntime) {
	t.Helper()
	f := &fakeRuntime{started: time.Now().Add(-time.Minute), stamp: stamp, damage: damage}
	opts := fixture(t)
	opts.runtimeEndpoint = startFake(t, f)
	var output bytes.Buffer
	if got := run(context.Background(), opts, strings.NewReader(requestJSON(f.started)), &output); got != 0 {
		t.Fatalf("run=%d: %s", got, output.String())
	}
	var result summary
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	return result, f
}

func TestForeignContainerIdentityNeverProducesCounters(t *testing.T) {
	for _, damage := range []string{"status_uid", "status_namespace", "status_name", "started", "created_zero", "created_after_started", "created_negative", "stats_uid", "stats_id"} {
		t.Run(damage, func(t *testing.T) {
			result, f := sample(t, damage, time.Now())
			if len(result.Pods) != 1 || len(result.Pods[0].Containers) != 0 || result.Pods[0].Ephemeral != nil {
				t.Fatalf("foreign identity accepted: %+v", result)
			}
			if len(result.Pods[0].Diagnostics) == 0 {
				t.Fatal("identity failure hidden")
			}
			f.mutex.Lock()
			defer f.mutex.Unlock()
			if strings.HasPrefix(damage, "status_") || strings.HasPrefix(damage, "created_") || damage == "started" {
				if len(f.calls) != 1 {
					t.Fatalf("unqualified identity reached Stats: %v", f.calls)
				}
			}
		})
	}
}

func TestUnknownCountersRemainAbsentAndDoNotFabricateZero(t *testing.T) {
	for _, damage := range []string{"rootfs_nil", "rootfs_value_nil", "rootfs_zero_timestamp", "memory_value_nil", "cpu_value_nil", "log_path"} {
		t.Run(damage, func(t *testing.T) {
			result, _ := sample(t, damage, time.Now())
			item := result.Pods[0]
			if len(item.Containers) != 1 {
				t.Fatalf("lost independently verified fields: %+v", item)
			}
			c := item.Containers[0]
			switch {
			case strings.HasPrefix(damage, "rootfs_"):
				if c.Rootfs != nil || item.Ephemeral != nil || c.Memory == nil || c.CPU == nil {
					t.Fatalf("unknown rootfs became a total: %+v", item)
				}
			case damage == "memory_value_nil":
				if c.Memory != nil || c.CPU == nil {
					t.Fatalf("unknown memory fabricated: %+v", c)
				}
			case damage == "cpu_value_nil":
				if c.CPU != nil || c.Memory == nil {
					t.Fatalf("unknown CPU fabricated: %+v", c)
				}
			case damage == "log_path":
				if c.Logs != nil || item.Ephemeral != nil {
					t.Fatalf("foreign log path counted: %+v", item)
				}
			}
		})
	}
	result, _ := sample(t, "valid_zero", time.Now())
	c := result.Pods[0].Containers[0]
	if c.Rootfs == nil || c.Rootfs.UsedBytes != 0 || c.CPU == nil || c.CPU.Cumulative == nil || *c.CPU.Cumulative != 0 || c.Memory == nil || c.Memory.Usage != 0 {
		t.Fatalf("measured zero lost: %+v", c)
	}
}

func TestStaleZeroAndFutureTimestampsNeverBecomeCurrentUsage(t *testing.T) {
	for _, stamp := range []time.Time{time.Unix(0, 0), time.Now().Add(-60 * time.Second), time.Now().Add(time.Minute)} {
		result, _ := sample(t, "", stamp)
		c := result.Pods[0].Containers[0]
		if c.CPU != nil || c.Memory != nil || c.Rootfs != nil || result.Pods[0].Ephemeral != nil {
			t.Fatalf("unqualified timestamp produced usage: %+v", result)
		}
	}
}

func TestRequestBoundariesRejectBeforeAnyRuntimeAccess(t *testing.T) {
	base := requestJSON(time.Now().Add(-time.Minute))
	tests := map[string]string{
		"oversize":      strings.Repeat(" ", maxPayload+1),
		"unknown_field": strings.Replace(base, `"namespace":"execution"`, `"namespace":"execution","private_path":"/secret"`, 1),
		"trailing":      base + "{}",
		"namespace":     strings.Replace(base, `"namespace":"execution"`, `"namespace":"../foreign"`, 1),
		"uid":           strings.Replace(base, podUID, "../../private", 1),
		"container_id":  strings.Replace(base, containerID, "../private", 1),
		"too_many_pods": `{"schema_version":"loom.node-resource-request.v1","namespace":"execution","pods":[` + strings.Repeat(`{"uid":"`+podUID+`","name":"pod","containers":[],"volumes":[]},`, 128) + `{"uid":"` + podUID + `","name":"pod","containers":[],"volumes":[]}]}`,
		"medium":        strings.Replace(base, `"volumes":[]`, `"volumes":[{"name":"workspace","medium":"HugePages"}]`, 1),
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			var output bytes.Buffer
			got := run(context.Background(), options{}, strings.NewReader(body), &output)
			if got == 0 || len(output.Bytes()) > maxPayload || strings.Contains(output.String(), "secret") || strings.Contains(output.String(), "private") {
				t.Fatalf("invalid request leaked or accepted: %d %s", got, output.String())
			}
			if name == "oversize" && !strings.Contains(output.String(), "request_too_large") {
				t.Fatal(output.String())
			}
		})
	}
}

func TestRuntimeAndBlockedInputRespectParentDeadline(t *testing.T) {
	f := &fakeRuntime{started: time.Now().Add(-time.Minute), stamp: time.Now(), delay: time.Second}
	opts := fixture(t)
	opts.runtimeEndpoint = startFake(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	var output bytes.Buffer
	before := time.Now()
	if got := run(ctx, opts, strings.NewReader(requestJSON(f.started)), &output); got == 0 || !strings.Contains(output.String(), "observation_deadline") || time.Since(before) > time.Second {
		t.Fatalf("deadline failed: %d %s", got, output.String())
	}
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel2()
	output.Reset()
	if got := run(ctx2, opts, reader, &output); got == 0 || !strings.Contains(output.String(), "observation_deadline") {
		t.Fatalf("blocked input escaped deadline: %d %s", got, output.String())
	}
}

func TestEmptyInventoryDoesNotConnectAndResponseIsBounded(t *testing.T) {
	var output bytes.Buffer
	opts := options{"unix:///tmp/no-runtime.sock", "/logs", "/pods"}
	got := run(context.Background(), opts, strings.NewReader(`{"schema_version":"loom.node-resource-request.v1","namespace":"execution","pods":[]}`), &output)
	if got != 0 || output.String() != "{\"pods\":[]}\n" {
		t.Fatalf("idle observer depended on runtime: %d %s", got, output.String())
	}
}

func Example_request() {
	req, code := decodeRequest(strings.NewReader(requestJSON(time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC))))
	fmt.Println(code == "", req.Namespace, len(req.Pods), req.Pods[0].Containers[0].ID == containerID)
	// Output: true execution 1 true
}

func TestOutputAndConfigurationBoundsDoNotExposeRawInput(t *testing.T) {
	var output bytes.Buffer
	if got := writeOutcome(&output, outcome{body: bytes.Repeat([]byte("private"), maxPayload)}); got == 0 || output.String() != "{\"error\":\"response_too_large\"}\n" {
		t.Fatalf("unbounded output: %d %d", got, output.Len())
	}
	for _, endpoint := range []string{"https://private.example.test", "unix://private.example.test/socket", "unix:relative", "unix:///tmp/socket?secret=private"} {
		output.Reset()
		got := run(context.Background(), options{endpoint, "/logs", "/pods"}, strings.NewReader(`{"schema_version":"loom.node-resource-request.v1","namespace":"execution","pods":[]}`), &output)
		if got == 0 || output.String() != "{\"error\":\"invalid_configuration\"}\n" {
			t.Fatalf("external runtime accepted: %d %s", got, output.String())
		}
	}
}

func TestRateRequiresEvidenceAndCounterOverflowRemainsUnknown(t *testing.T) {
	zero, _ := sample(t, "rate_zero", time.Now())
	if zero.Pods[0].Containers[0].CPU.Rate != nil {
		t.Fatal("first-read CRI zero rate masqueraded as measured rate")
	}
	positive, _ := sample(t, "rate_positive", time.Now())
	rate := positive.Pods[0].Containers[0].CPU.Rate
	if rate == nil || *rate != 1000 {
		t.Fatalf("valid runtime rate lost: %v", rate)
	}
	overflow, _ := sample(t, "counter_overflow", time.Now())
	item := overflow.Pods[0]
	c := item.Containers[0]
	if c.CPU != nil || c.Memory != nil || c.Rootfs != nil || item.Ephemeral != nil {
		t.Fatalf("out-of-ledger-range counters accepted: %+v", item)
	}
	oversized, _ := sample(t, "oversize_stats", time.Now())
	if len(oversized.Pods[0].Containers) != 0 || oversized.Pods[0].Ephemeral != nil {
		t.Fatalf("oversized CRI reply accepted: %+v", oversized)
	}
}

func TestFilesystemTimesPreserveOldestCRIConstituent(t *testing.T) {
	old := time.Now().Add(-30 * time.Second)
	result, _ := sample(t, "", old)
	pod := result.Pods[0]
	c := pod.Containers[0]
	if c.Rootfs == nil || c.Rootfs.Time != old.UTC().Format(time.RFC3339Nano) {
		t.Fatalf("rootfs source time refreshed: %+v", c.Rootfs)
	}
	logs, err := time.Parse(time.RFC3339Nano, c.Logs.Time)
	if err != nil || !logs.After(old) {
		t.Fatalf("missing actual log scan time: %+v", c.Logs)
	}
	if pod.Ephemeral == nil || pod.Ephemeral.Time != c.Rootfs.Time {
		t.Fatalf("Pod total hid old constituent: %+v", pod.Ephemeral)
	}
}

func (f *fakeRuntime) ListContainerStats(ctx context.Context, in *runtime.ListContainerStatsRequest) (*runtime.ListContainerStatsResponse, error) {
	f.mutex.Lock()
	f.calls = append(f.calls, "liststats")
	f.mutex.Unlock()
	if in.Filter == nil || in.Filter.Id != containerID || in.Filter.PodSandboxId != "" || len(in.Filter.LabelSelector) != 0 {
		return nil, status.Error(codes.InvalidArgument, "private invalid filter")
	}
	item := f.statsReply().Stats
	item.Cpu = nil
	item.Memory = nil // Exited task/cgroup gone; snapshot still retained.
	reply := &runtime.ListContainerStatsResponse{Stats: []*runtime.ContainerStats{item}}
	switch f.damage {
	case "exited_nil":
		reply.Stats = []*runtime.ContainerStats{nil}
	case "exited_foreign_name":
		item.Attributes.Metadata.Name = "foreign"
	case "exited_empty":
		reply.Stats = nil
	case "exited_multiple":
		reply.Stats = append(reply.Stats, item)
	case "exited_foreign":
		item.Attributes.Id = strings.Repeat("b", 64)
	case "exited_foreign_uid":
		item.Attributes.Labels["io.kubernetes.pod.uid"] = "22222222-2222-4222-8222-222222222222"
	}
	return reply, nil
}

func TestExitedInitRetainsMeasuredRootfsWithoutInventedCPUOrMemory(t *testing.T) {
	result, f := sample(t, "exited", time.Now())
	if len(result.Pods) != 1 || len(result.Pods[0].Containers) != 1 {
		t.Fatalf("exited init lost retained snapshot: %+v", result)
	}
	item := result.Pods[0]
	c := item.Containers[0]
	if c.CPU != nil || c.Memory != nil || c.Rootfs == nil || c.Rootfs.UsedBytes != 8192 || item.Ephemeral == nil {
		t.Fatalf("exited init values fabricated or disk omitted: %+v", item)
	}
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if strings.Join(f.calls, ",") != "status,liststats" {
		t.Fatalf("exited init used task-dependent stats: %v", f.calls)
	}
}

func TestExitedStatsRequireOneBoundReplyAndKnownFreshRootfs(t *testing.T) {
	for _, damage := range []string{"exited_nil", "exited_foreign_name", "exited_empty", "exited_multiple", "exited_foreign", "exited_foreign_uid", "exited_rootfs_nil", "exited_rootfs_value_nil", "exited_rootfs_zero_timestamp"} {
		t.Run(damage, func(t *testing.T) {
			result, _ := sample(t, damage, time.Now())
			item := result.Pods[0]
			if item.Ephemeral != nil {
				t.Fatalf("unknown exited rootfs reported as complete Pod total: %+v", item)
			}
			if len(item.Containers) > 0 && (item.Containers[0].CPU != nil || item.Containers[0].Memory != nil) {
				t.Fatal("exited CPU/memory invented")
			}
		})
	}
}

func TestRemovedInitOrStaleRetainedSnapshotNeverBecomesZeroRootfs(t *testing.T) {
	removed, f := sample(t, "status_not_found", time.Now())
	if len(removed.Pods[0].Containers) != 0 || removed.Pods[0].Ephemeral != nil {
		t.Fatalf("removed metadata fabricated zero snapshot: %+v", removed)
	}
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if strings.Join(f.calls, ",") != "status" {
		t.Fatalf("unknown init reached other runtime RPC: %v", f.calls)
	}
	stale, _ := sample(t, "exited", time.Now().Add(-60*time.Second))
	if stale.Pods[0].Containers[0].Rootfs != nil || stale.Pods[0].Ephemeral != nil {
		t.Fatalf("stale exited snapshot refreshed: %+v", stale)
	}
}

func TestCreationTimeMatchesKubeletJSONWhileStartedAtRemainsPrivate(t *testing.T) {
	created := time.Now().UTC().Add(-time.Minute).Truncate(time.Second).Add(123456789 * time.Nanosecond)
	started := created.Add(3*time.Second + 777*time.Nanosecond)
	stamp := time.Now().UTC().Truncate(time.Second).Add(987654321 * time.Nanosecond)
	f := &fakeRuntime{created: created, started: started, stamp: stamp}
	opts := fixture(t)
	opts.runtimeEndpoint = startFake(t, f)
	var output bytes.Buffer
	if run(context.Background(), opts, strings.NewReader(requestJSON(started)), &output) != 0 {
		t.Fatal(output.String())
	}
	var value struct {
		Pods []struct {
			Containers []map[string]any `json:"containers"`
		} `json:"pods"`
	}
	if err := json.Unmarshal(output.Bytes(), &value); err != nil {
		t.Fatal(err)
	}
	container := value.Pods[0].Containers[0]
	if container["startTime"] != created.Format(time.RFC3339) {
		t.Fatalf("incarnation is not kubelet creation-second identity: %v", container["startTime"])
	}
	if container["_started_at"] != started.Format(time.RFC3339Nano) {
		t.Fatalf("actual started identity missing or truncated: %v", container["_started_at"])
	}
	if container["cpu"].(map[string]any)["time"] != stamp.Format(time.RFC3339Nano) {
		t.Fatal("original counter timestamp precision was changed")
	}
}
