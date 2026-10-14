package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestMaterializeInputsStreamsAndVerifiesImmutableBundle(t *testing.T) {
	files := []struct {
		path string
		body []byte
		mode string
	}{
		{path: "instruction.md", body: []byte("answer the question\n"), mode: "0644"},
		{path: "verifier/check.sh", body: []byte("#!/bin/sh\nexit 0\n"), mode: "0755"},
	}
	bundleDigest := sha256.New()
	manifestFiles := make([]taskInputFile, 0, len(files))
	for _, file := range files {
		digest := sha256.Sum256(file.body)
		manifestFiles = append(manifestFiles, taskInputFile{
			RelativePath: file.path,
			SizeBytes:    int64(len(file.body)),
			SHA256:       "sha256:" + hex.EncodeToString(digest[:]),
			Mode:         file.mode,
		})
		bundleDigest.Write([]byte{0})
		bundleDigest.Write([]byte(file.path))
		bundleDigest.Write([]byte{0})
		bundleDigest.Write(file.body)
	}
	revision := "sha256:" + hex.EncodeToString(bundleDigest.Sum(nil))
	manifestBytes, err := json.Marshal(taskInputManifest{
		SchemaVersion:      "loom.service-execution-input-manifest.v1",
		TaskRevisionSHA256: revision,
		Files:              manifestFiles,
	})
	if err != nil {
		t.Fatal(err)
	}
	manifestDigest := sha256.Sum256(manifestBytes)
	leaseID := "0194d739-8bec-7b7b-88f5-62f7cbd42cb3"
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("X-Loom-Execution-Lease-Id") != leaseID ||
			request.Header.Get("X-Loom-Execution-Generation") != "7" ||
			request.Header.Get("X-Loom-Execution-Role") != "attempt" {
			http.Error(writer, "identity mismatch", http.StatusForbidden)
			return
		}
		switch request.URL.Path {
		case "/internal/service-execution/inputs/manifest":
			_, _ = writer.Write(manifestBytes)
		case "/internal/service-execution/inputs/files/0":
			_, _ = writer.Write(files[0].body)
		case "/internal/service-execution/inputs/files/1":
			_, _ = writer.Write(files[1].body)
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()
	root, err := url.Parse(server.URL + "/internal/service-execution")
	if err != nil {
		t.Fatal(err)
	}
	broker := &workloadBroker{
		root: root,
		identity: workloadIdentity{
			LeaseID: leaseID, Generation: 7, ExecutionRole: "attempt",
		},
		client: server.Client(),
	}
	workspace := t.TempDir()
	p := testPlan("/workspace", phase{
		Role: "agent", Argv: []string{"/bin/true"}, WorkingDirectory: "/workspace", TimeoutSeconds: 1,
	})
	p.TaskRevisionSHA256 = revision
	p.TaskInput = &taskInput{
		SchemaVersion:  "loom.runtime-task-input.v1",
		ManifestSHA256: "sha256:" + hex.EncodeToString(manifestDigest[:]),
		FileCount:      len(files),
		TotalBytes:     int64(len(files[0].body) + len(files[1].body)),
	}
	if err := broker.materializeInputs(context.Background(), p, workspace); err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		path := filepath.Join(workspace, filepath.FromSlash(file.path))
		body, err := os.ReadFile(path)
		if err != nil || string(body) != string(file.body) {
			t.Fatalf("materialized file mismatch path=%s body=%q err=%v", path, body, err)
		}
		info, err := os.Stat(path)
		expectedMode := os.FileMode(0o644)
		if file.mode == "0755" {
			expectedMode = 0o755
		}
		if err != nil || info.Mode().Perm() != expectedMode {
			t.Fatalf("materialized mode mismatch path=%s mode=%v err=%v", path, info.Mode(), err)
		}
	}
}

func TestDecodeTaskInputManifestRejectsBindingDrift(t *testing.T) {
	p := testPlan("/workspace", phase{
		Role: "agent", Argv: []string{"/bin/true"}, WorkingDirectory: "/workspace", TimeoutSeconds: 1,
	})
	p.TaskInput = &taskInput{
		SchemaVersion:  "loom.runtime-task-input.v1",
		ManifestSHA256: "sha256:" + strings.Repeat("0", 64),
		FileCount:      1,
		TotalBytes:     1,
	}
	if _, err := decodeInputManifest([]byte("{}"), p, p.TaskInput, false); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("manifest digest drift was accepted: %v", err)
	}
}

func TestMaterializeInputsRetriesKnownIdentityAvailabilityErrors(t *testing.T) {
	for _, reason := range []string{"workload_identity_not_observed", "execution_pod_review_unavailable", "execution_target_unavailable"} {
		t.Run(reason, func(t *testing.T) {
			revisionDigest := sha256.Sum256(nil)
			revision := "sha256:" + hex.EncodeToString(revisionDigest[:])
			manifestBytes, err := json.Marshal(taskInputManifest{
				SchemaVersion:      "loom.service-execution-input-manifest.v1",
				TaskRevisionSHA256: revision,
				Files:              []taskInputFile{},
			})
			if err != nil {
				t.Fatal(err)
			}
			manifestDigest := sha256.Sum256(manifestBytes)
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if calls.Add(1) == 1 {
					payload, _ := json.Marshal(map[string]string{"detail": reason})
					http.Error(writer, string(payload), http.StatusServiceUnavailable)
					return
				}
				_, _ = writer.Write(manifestBytes)
			}))
			defer server.Close()
			root, err := url.Parse(server.URL + "/internal/service-execution")
			if err != nil {
				t.Fatal(err)
			}
			broker := &workloadBroker{
				root: root,
				identity: workloadIdentity{
					LeaseID: "0194d739-8bec-7b7b-88f5-62f7cbd42cb3", Generation: 7, ExecutionRole: "attempt",
				},
				client: server.Client(),
			}
			p := testPlan("/workspace", phase{
				Role: "agent", Argv: []string{"/bin/true"}, WorkingDirectory: "/workspace", TimeoutSeconds: 1,
			})
			p.TaskRevisionSHA256 = revision
			p.TaskInput = &taskInput{
				SchemaVersion:  "loom.runtime-task-input.v1",
				ManifestSHA256: "sha256:" + hex.EncodeToString(manifestDigest[:]),
				FileCount:      0,
				TotalBytes:     0,
			}
			if err := broker.materializeInputs(context.Background(), p, t.TempDir()); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 2 {
				t.Fatalf("expected one workload identity retry, got %d requests", calls.Load())
			}
		})
	}
}

func TestWorkloadIdentityRetryClassificationIsNarrow(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "identity observation race",
			err:  &brokerHTTPError{statusCode: http.StatusServiceUnavailable, body: `{"detail":"workload_identity_not_observed"}`},
			want: true,
		},
		{
			name: "unknown unavailable response",
			err:  &brokerHTTPError{statusCode: http.StatusServiceUnavailable, body: `{"detail":"unknown_unavailability"}`},
		},
		{
			name: "permission failure",
			err:  &brokerHTTPError{statusCode: http.StatusForbidden, body: `{"detail":"workload_identity_not_observed"}`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := workloadIdentityTemporarilyUnavailable(test.err); got != test.want {
				t.Fatalf("retry classification = %t, want %t", got, test.want)
			}
		})
	}
}

func TestInputIdentityDenialAndFenceConflictAreNotRetried(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusConflict} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				calls.Add(1)
				// A retryable-looking reason cannot override the status code.
				http.Error(writer, `{"detail":"execution_pod_review_unavailable"}`, status)
			}))
			defer server.Close()
			root, err := url.Parse(server.URL)
			if err != nil {
				t.Fatal(err)
			}
			broker := &workloadBroker{root: root, client: server.Client()}
			for _, path := range []string{"/inputs/manifest", "/inputs/files/0"} {
				_, err := broker.getInput(context.Background(), broker.endpoint(path))
				if err == nil {
					t.Fatal("authorization/fence failure was ignored")
				}
			}
			if calls.Load() != 2 {
				t.Fatalf("denial/fence requests retried: calls=%d", calls.Load())
			}
		})
	}
}
