package main

import (
	"context"
	"math"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"google.golang.org/grpc"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type statsRuntime interface {
	ContainerStatus(context.Context, *runtime.ContainerStatusRequest, ...grpc.CallOption) (*runtime.ContainerStatusResponse, error)
	ContainerStats(context.Context, *runtime.ContainerStatsRequest, ...grpc.CallOption) (*runtime.ContainerStatsResponse, error)
	ListContainerStats(context.Context, *runtime.ListContainerStatsRequest, ...grpc.CallOption) (*runtime.ListContainerStatsResponse, error)
}
type summary struct {
	Pods []podStats `json:"pods"`
}
type podReference struct {
	UID       string `json:"uid"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}
type podStats struct {
	PodRef      podReference     `json:"podRef"`
	Containers  []containerStats `json:"containers"`
	Ephemeral   *fsStats         `json:"ephemeral-storage,omitempty"`
	Diagnostics []string         `json:"_diagnostic_codes,omitempty"`
}
type containerStats struct {
	Name      string       `json:"name"`
	StartTime string       `json:"startTime"`
	StartedAt string       `json:"_started_at"`
	ID        string       `json:"_container_id"`
	CPU       *cpuStats    `json:"cpu,omitempty"`
	Memory    *memoryStats `json:"memory,omitempty"`
	Rootfs    *fsStats     `json:"rootfs,omitempty"`
	Logs      *fsStats     `json:"logs,omitempty"`
}
type fsStats struct {
	UsedBytes uint64 `json:"usedBytes"`
	Time      string `json:"time,omitempty"`
}
type cpuStats struct {
	Time       string  `json:"time"`
	Cumulative *uint64 `json:"usageCoreNanoSeconds,omitempty"`
	Rate       *uint64 `json:"usageNanoCores,omitempty"`
}
type memoryStats struct {
	Time  string `json:"time"`
	Usage uint64 `json:"usageBytes"`
}

func collect(ctx context.Context, req request, client statsRuntime, opts options, now func() time.Time) summary {
	result := summary{Pods: make([]podStats, 0, len(req.Pods))}
	for _, pod := range req.Pods {
		item := podStats{PodRef: podReference{pod.UID, req.Namespace, pod.Name}, Containers: []containerStats{}}
		diagnostics := map[string]bool{}
		complete := true
		var rootfsTotal uint64
		ephemeralAt := now()
		for _, container := range pod.Containers {
			if ctx.Err() != nil {
				diagnostics["observation_deadline"] = true
				complete = false
				break
			}
			statusReply, err := client.ContainerStatus(ctx, &runtime.ContainerStatusRequest{ContainerId: container.ID})
			if err != nil || statusReply == nil || statusReply.Status == nil {
				diagnostics["container_status_unavailable"] = true
				complete = false
				continue
			}
			status := statusReply.Status
			started, _ := time.Parse(time.RFC3339Nano, container.StartedAt)
			if status.Id != container.ID || status.Metadata == nil || status.Metadata.Name != container.Name || !boundLabels(status.Labels, req.Namespace, pod) {
				diagnostics["container_identity_mismatch"] = true
				complete = false
				continue
			}
			if status.StartedAt <= 0 || time.Unix(0, status.StartedAt).Unix() != started.Unix() {
				diagnostics["container_start_mismatch"] = true
				complete = false
				continue
			}
			if status.CreatedAt <= 0 || status.CreatedAt > status.StartedAt {
				diagnostics["container_creation_mismatch"] = true
				complete = false
				continue
			}
			if ctx.Err() != nil {
				diagnostics["observation_deadline"] = true
				complete = false
				break
			}
			var stats *runtime.ContainerStats
			if status.State == runtime.ContainerState_CONTAINER_EXITED {
				// containerd's single Stats path requires a live task metric.
				// The filtered list path can still return the retained writable
				// snapshot when an exited init has no cgroup CPU/memory data.
				// No broad inventory or unrelated container data is accepted.
				reply, err := client.ListContainerStats(ctx, &runtime.ListContainerStatsRequest{
					Filter: &runtime.ContainerStatsFilter{Id: container.ID},
				})
				if err == nil && reply != nil && len(reply.Stats) == 1 {
					stats = reply.Stats[0]
				}
			} else {
				reply, err := client.ContainerStats(ctx, &runtime.ContainerStatsRequest{ContainerId: container.ID})
				if err == nil && reply != nil {
					stats = reply.Stats
				}
			}
			if stats == nil {
				diagnostics["container_stats_unavailable"] = true
				complete = false
				continue
			}
			if stats.Attributes == nil || stats.Attributes.Id != container.ID || stats.Attributes.Metadata == nil || stats.Attributes.Metadata.Name != container.Name || stats.Attributes.Metadata.Attempt != status.Metadata.Attempt || !boundLabels(stats.Attributes.Labels, req.Namespace, pod) {
				diagnostics["container_identity_mismatch"] = true
				complete = false
				continue
			}
			// Match kubelet Summary's metav1.Time JSON: creation epoch at second
			// precision. Actual StartedAt separately fences the API incarnation.
			out := containerStats{Name: container.Name,
				StartTime: time.Unix(0, status.CreatedAt).UTC().Format(time.RFC3339),
				StartedAt: time.Unix(0, status.StartedAt).UTC().Format(time.RFC3339Nano),
				ID:        "containerd://" + container.ID}
			stamp := now()
			if stats.Cpu != nil && fresh(stats.Cpu.Timestamp, stamp) && (stats.Cpu.UsageCoreNanoSeconds != nil || stats.Cpu.UsageNanoCores != nil) {
				out.CPU = &cpuStats{Time: time.Unix(0, stats.Cpu.Timestamp).UTC().Format(time.RFC3339Nano)}
				if stats.Cpu.UsageCoreNanoSeconds != nil && stats.Cpu.UsageCoreNanoSeconds.Value <= math.MaxInt64 {
					value := stats.Cpu.UsageCoreNanoSeconds.Value
					out.CPU.Cumulative = &value
				}
				// A zero rate can be containerd's first-read placeholder. The Python
				// observer derives an honest zero from two cumulative observations.
				if stats.Cpu.UsageNanoCores != nil && stats.Cpu.UsageNanoCores.Value > 0 && stats.Cpu.UsageNanoCores.Value <= math.MaxInt64 {
					value := stats.Cpu.UsageNanoCores.Value
					out.CPU.Rate = &value
				}
				if out.CPU.Cumulative == nil && out.CPU.Rate == nil {
					out.CPU = nil
					diagnostics["cpu_sample_unavailable"] = true
				}
			} else {
				diagnostics["cpu_sample_unavailable"] = true
			}
			if stats.Memory != nil && fresh(stats.Memory.Timestamp, stamp) && stats.Memory.UsageBytes != nil && stats.Memory.UsageBytes.Value <= math.MaxInt64 {
				out.Memory = &memoryStats{time.Unix(0, stats.Memory.Timestamp).UTC().Format(time.RFC3339Nano), stats.Memory.UsageBytes.Value}
			} else {
				diagnostics["memory_sample_unavailable"] = true
			}
			if stats.WritableLayer != nil && fresh(stats.WritableLayer.Timestamp, stamp) && stats.WritableLayer.UsedBytes != nil && stats.WritableLayer.UsedBytes.Value <= math.MaxInt64 {
				rootfsAt := time.Unix(0, stats.WritableLayer.Timestamp).UTC()
				out.Rootfs = &fsStats{UsedBytes: stats.WritableLayer.UsedBytes.Value, Time: rootfsAt.Format(time.RFC3339Nano)}
				if rootfsAt.Before(ephemeralAt) {
					ephemeralAt = rootfsAt
				}
				if math.MaxInt64-rootfsTotal < out.Rootfs.UsedBytes {
					complete = false
					diagnostics["filesystem_overflow"] = true
				} else {
					rootfsTotal += out.Rootfs.UsedBytes
				}
			} else {
				diagnostics["rootfs_sample_unavailable"] = true
				complete = false
			}
			logPod := req.Namespace + "_" + pod.Name + "_" + pod.UID
			// The operator mounts the configured host log root. Runtime paths
			// must match that Pod/container/attempt suffix, never select a host path.
			expected := filepath.Join(logPod, container.Name, strconv.FormatUint(uint64(status.Metadata.Attempt), 10)+".log")
			if !validLogPath(status.LogPath, expected) {
				diagnostics["container_logs_unavailable"] = true
				complete = false
			} else {
				logAt := now()
				usage, err := containerLogUsage(ctx, opts.podLogsRoot, logPod, container.Name)
				if err == nil {
					out.Logs = &fsStats{UsedBytes: usage, Time: logAt.UTC().Format(time.RFC3339Nano)}
				} else {
					diagnostics["container_logs_unavailable"] = true
					complete = false
				}
			}
			item.Containers = append(item.Containers, out)
		}
		disk, code := podDiskUsage(ctx, opts, req.Namespace, pod)
		if code != "" {
			diagnostics[code] = true
			complete = false
		}
		if complete && math.MaxInt64-rootfsTotal >= disk {
			item.Ephemeral = &fsStats{UsedBytes: rootfsTotal + disk, Time: ephemeralAt.UTC().Format(time.RFC3339Nano)}
		} else if complete {
			diagnostics["filesystem_overflow"] = true
		}
		for code := range diagnostics {
			item.Diagnostics = append(item.Diagnostics, code)
		}
		sort.Strings(item.Diagnostics)
		result.Pods = append(result.Pods, item)
		if ctx.Err() != nil {
			break
		}
	}
	return result
}

func boundLabels(labels map[string]string, namespace string, pod podRequest) bool {
	return labels["io.kubernetes.pod.uid"] == pod.UID && labels["io.kubernetes.pod.namespace"] == namespace && labels["io.kubernetes.pod.name"] == pod.Name
}
func fresh(timestamp int64, now time.Time) bool {
	if timestamp <= 0 {
		return false
	}
	age := now.Sub(time.Unix(0, timestamp))
	return age >= -5*time.Second && age < 60*time.Second
}
func validLogPath(path, expected string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path && len(path) > len(expected) && path[len(path)-len(expected)-1:] == "/"+expected
}
