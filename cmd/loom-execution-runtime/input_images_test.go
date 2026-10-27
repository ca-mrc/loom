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

func TestPreparedImagesStreamFromAuthenticatedSourceAndRejectDrift(t *testing.T) {
	for _, damage := range []string{"", "image", "bytes", "service"} {
		t.Run(damage, func(t *testing.T) {
			body := []byte("immutable-oci-archive")
			checksum := sha256.Sum256(body)
			ref := "registry/image@sha256:" + strings.Repeat("a", 64)
			inventory := map[string]preparedImageInput{"main": {ImageRef: ref, SizeBytes: int64(len(body)), SHA256: "sha256:" + hex.EncodeToString(checksum[:])}}
			if damage == "image" {
				item := inventory["main"]
				item.ImageRef = "registry/other@sha256:" + strings.Repeat("b", 64)
				inventory["main"] = item
			}
			if damage == "service" {
				inventory["foreign"] = inventory["main"]
				delete(inventory, "main")
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("X-Loom-Execution-Lease-Id") != "lease" || r.Header.Get("X-Loom-Execution-Role") != "attempt" {
					t.Error("missing scoped identity")
				}
				if strings.HasSuffix(r.URL.Path, "manifest") {
					json.NewEncoder(w).Encode(inventory)
					return
				}
				if damage == "bytes" {
					w.Write([]byte("changed"))
					return
				}
				w.Write(body)
			}))
			defer server.Close()
			root, _ := url.Parse(server.URL + "/internal/service-execution")
			broker := &workloadBroker{root: root, identity: workloadIdentity{LeaseID: "lease", Generation: 1, ExecutionRole: "attempt"}, client: server.Client()}
			p := preparedTaskPlan()
			env, _ := json.Marshal(map[string]any{"storage_mb": 1, "native_compose": map[string]any{"services": []map[string]string{{"name": "main", "image": ref}}}})
			p.Main.Environment = map[string]string{"LOOM_TASK_ENVIRONMENT_JSON": string(env)}
			destination := t.TempDir()
			err := broker.materializePreparedImages(context.Background(), p, destination)
			if damage != "" {
				if err == nil {
					t.Fatal("drift accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(filepath.Join(destination, "main.tar"))
			if err != nil || string(got) != string(body) {
				t.Fatal("prepared bytes changed")
			}
		})
	}
}
