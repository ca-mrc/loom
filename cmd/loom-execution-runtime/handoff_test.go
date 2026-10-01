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
	"testing"
)

func handoffFixture(t *testing.T, paths []string, mode string) (plan, *workloadBroker, func()) {
	t.Helper()
	bodies := map[string][]byte{}
	files := make([]taskInputFile, 0, len(paths))
	total := int64(0)
	for _, path := range paths {
		body := []byte("handoff " + path)
		digest := sha256.Sum256(body)
		bodies[path] = body
		files = append(files, taskInputFile{
			RelativePath: path, SizeBytes: int64(len(body)),
			SHA256: "sha256:" + hex.EncodeToString(digest[:]), Mode: mode,
		})
		total += int64(len(body))
	}
	revision := "sha256:" + strings.Repeat("a", 64)
	manifestBytes, err := json.Marshal(taskInputManifest{
		SchemaVersion: "loom.service-execution-input-manifest.v1", TaskRevisionSHA256: revision, Files: files,
	})
	if err != nil {
		t.Fatal(err)
	}
	manifestDigest := sha256.Sum256(manifestBytes)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("X-Loom-Execution-Role") != "verifier" {
			http.Error(writer, "identity mismatch", http.StatusForbidden)
			return
		}
		switch request.URL.Path {
		case "/internal/service-execution/inputs/handoff/manifest":
			_, _ = writer.Write(manifestBytes)
		default:
			for index, file := range files {
				if request.URL.Path == "/internal/service-execution/inputs/handoff/files/"+string(rune('0'+index)) {
					_, _ = writer.Write(bodies[file.RelativePath])
					return
				}
			}
			http.NotFound(writer, request)
		}
	}))
	root, err := url.Parse(server.URL + "/internal/service-execution")
	if err != nil {
		t.Fatal(err)
	}
	broker := &workloadBroker{
		root:     root,
		identity: workloadIdentity{LeaseID: "0194d739-8bec-7b7b-88f5-62f7cbd42cb3", Generation: 1, ExecutionRole: "verifier"},
		client:   server.Client(),
	}
	p := testPlan("/workspace", phase{
		Role: "verifier", Argv: []string{"/bin/true"}, WorkingDirectory: "/workspace", TimeoutSeconds: 1,
	})
	p.TaskRevisionSHA256 = revision
	p.HandoffInput = &taskInput{
		SchemaVersion:  "loom.runtime-handoff-input.v1",
		ManifestSHA256: "sha256:" + hex.EncodeToString(manifestDigest[:]),
		FileCount:      len(files),
		TotalBytes:     total,
	}
	return p, broker, server.Close
}

func TestMaterializeHandoffStagesCommittedWorkspaceUnderPrivateDirectory(t *testing.T) {
	p, broker, stop := handoffFixture(t, []string{".loom/mutable-paths/manifest.json", ".loom/workspace.tar"}, "0644")
	defer stop()
	workspace := t.TempDir()
	if err := broker.materializeHandoff(context.Background(), p, workspace); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(workspace, ".loom", "workspace.tar"))
	if err != nil || string(body) != "handoff .loom/workspace.tar" {
		t.Fatalf("handoff archive mismatch body=%q err=%v", body, err)
	}
}

func TestMaterializeHandoffRejectsPathsOutsidePrivateDirectory(t *testing.T) {
	for name, files := range map[string]struct {
		paths []string
		mode  string
	}{
		"task path":  {paths: []string{"tests/test.sh"}, mode: "0644"},
		"executable": {paths: []string{".loom/workspace.tar"}, mode: "0755"},
	} {
		t.Run(name, func(t *testing.T) {
			p, broker, stop := handoffFixture(t, files.paths, files.mode)
			defer stop()
			err := broker.materializeHandoff(context.Background(), p, t.TempDir())
			if err == nil || !strings.Contains(err.Error(), "inventory is invalid") {
				t.Fatalf("unsafe handoff was accepted: %v", err)
			}
		})
	}
}

func TestHandoffBindingBelongsOnlyToDeferredVerifier(t *testing.T) {
	p, _, stop := handoffFixture(t, []string{".loom/workspace.tar"}, "0644")
	defer stop()
	p.ExecutionRole = "attempt"
	p.Main.Role = "agent"
	if err := p.validate(); err == nil || !strings.Contains(err.Error(), "handoff") {
		t.Fatalf("attempt handoff binding was accepted: %v", err)
	}
}
