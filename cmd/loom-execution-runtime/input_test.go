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
	"strconv"
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

// inputMaterializationFixture serves the immutable flat-sorted wire inventory
// by its original index, independently of the order the runtime downloads it.
func inputMaterializationFixture(t *testing.T, manifest taskInputManifest, bodies [][]byte) (*workloadBroker, plan) {
	t.Helper()
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestDigest := sha256.Sum256(manifestBytes)
	const prefix = "/internal/service-execution/inputs/"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == prefix+"manifest" {
			_, _ = w.Write(manifestBytes)
			return
		}
		index, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, prefix+"files/"))
		if err != nil || index < 0 || index >= len(bodies) {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(bodies[index])
	}))
	t.Cleanup(server.Close)
	root, err := url.Parse(server.URL + "/internal/service-execution")
	if err != nil {
		t.Fatal(err)
	}
	broker := &workloadBroker{root: root, client: server.Client()}
	p := testPlan("/workspace", phase{
		Role: "agent", Argv: []string{"/bin/true"}, WorkingDirectory: "/workspace", TimeoutSeconds: 1,
	})
	p.TaskRevisionSHA256 = manifest.TaskRevisionSHA256
	p.TaskInput = &taskInput{
		SchemaVersion: "loom.runtime-task-input.v1", ManifestSHA256: "sha256:" + hex.EncodeToString(manifestDigest[:]),
		FileCount: len(manifest.Files),
	}
	for _, file := range manifest.Files {
		p.TaskInput.TotalBytes += file.SizeBytes
	}
	return broker, p
}

func TestMaterializeInputsPreservesCanonicalPathComponentOrder(t *testing.T) {
	// Fixed expected value produced by loom_bundle_checksum.sha256_of_dir over
	// these exact files. Do not derive it from the transport inventory order:
	// pathlib sorts src/client/format.ts before src/client-entry.ts, while the
	// canonical wire manifest deliberately has the opposite order.
	const revision = "sha256:46786990a3099a1bb2289eea2cca7321b0f7c964f03ad883a0578bd7827b63c8"
	files := []struct{ path, body string }{
		{"src/.meta.json", "metadata\n"},
		{"src/.meta/info", "info\n"},
		{"src/client-entry.ts", "entry\n"},
		{"src/client/format.ts", "format\n"},
		{"src/client/render.ts", "render\n"},
		{"src/client0.ts", "sibling\n"},
		{"src/server-entry.ts", "server entry\n"},
		{"src/server/handler.ts", "handler\n"},
	}
	for _, test := range []struct{ name, wantError string }{
		{"canonical", ""},
		{"truncated", "task input file identity mismatch"},
		{"oversized", "task input file identity mismatch"},
		{"altered-content", "task input file identity mismatch"},
		{"bundle-mismatch", "materialized task bundle checksum mismatch"},
		{"noncanonical-wire-order", "task input file inventory is invalid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			manifest := taskInputManifest{SchemaVersion: "loom.service-execution-input-manifest.v1", TaskRevisionSHA256: revision}
			bodies := make([][]byte, 0, len(files))
			for _, file := range files {
				body := []byte(file.body)
				digest := sha256.Sum256(body)
				manifest.Files = append(manifest.Files, taskInputFile{
					RelativePath: file.path, SizeBytes: int64(len(body)),
					SHA256: "sha256:" + hex.EncodeToString(digest[:]), Mode: "0644",
				})
				bodies = append(bodies, body)
			}
			switch test.name {
			case "truncated":
				bodies[0] = bodies[0][:len(bodies[0])-1]
			case "oversized":
				bodies[0] = append(bodies[0], 'x')
			case "altered-content":
				bodies[0][0] ^= 1
			case "bundle-mismatch":
				manifest.TaskRevisionSHA256 = "sha256:" + strings.Repeat("0", 64)
			case "noncanonical-wire-order":
				manifest.Files[0], manifest.Files[1] = manifest.Files[1], manifest.Files[0]
			}
			broker, p := inputMaterializationFixture(t, manifest, bodies)
			workspace := t.TempDir()
			err := broker.materializeInputs(context.Background(), p, workspace)
			if test.wantError != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					t.Fatalf("want %q, got %v", test.wantError, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for _, file := range files {
				body, err := os.ReadFile(filepath.Join(workspace, filepath.FromSlash(file.path)))
				if err != nil || string(body) != file.body {
					t.Fatalf("materialized %s: body=%q err=%v", file.path, body, err)
				}
			}
		})
	}
}
