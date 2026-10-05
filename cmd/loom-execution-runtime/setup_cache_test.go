package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func cacheBroker(t *testing.T, handler http.HandlerFunc) *workloadBroker {
	t.Helper()
	gateway := httptest.NewServer(handler)
	t.Cleanup(gateway.Close)
	root, _ := url.Parse(gateway.URL + "/internal/service-execution")
	tokenFile := filepath.Join(t.TempDir(), "pod-token")
	if err := os.WriteFile(tokenFile, []byte("pod-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	return &workloadBroker{podTokenFile: tokenFile, root: root, client: gateway.Client(),
		identity: workloadIdentity{LeaseID: "lease-one", Generation: 1, ExecutionRole: "attempt"}}
}

func cachePlan() plan {
	p := testPlan("/workspace", phase{Role: "agent", Argv: []string{"true"}, WorkingDirectory: "/workspace", TimeoutSeconds: 2})
	p.Setup = []phase{{Role: "setup", Argv: []string{"true"}, WorkingDirectory: "/workspace", TimeoutSeconds: 2}}
	p.SetupCache = &setupCache{SchemaVersion: "loom.runtime-setup-cache.v1", IdentitySHA256: "sha256:" + strings.Repeat("a", 64),
		InstallRoot: "/opt/loom-harness/codex", MaxBytes: 1024}
	return p
}

func digestOf(payload []byte) string {
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func TestSetupCachePlanContract(t *testing.T) {
	valid := cachePlan()
	raw, _ := json.Marshal(valid)
	if _, err := decodePlan(raw); err != nil {
		t.Fatal(err)
	}
	legacy := testPlan("/workspace", phase{Role: "agent", Argv: []string{"true"}, WorkingDirectory: "/workspace", TimeoutSeconds: 2})
	raw, _ = json.Marshal(legacy)
	if strings.Contains(string(raw), "setup_cache") {
		t.Fatal("legacy plan bytes changed")
	}
	for name, mutate := range map[string]func(*plan){
		"no setup phase": func(p *plan) { p.Setup = nil },
		"relative root":  func(p *plan) { p.SetupCache.InstallRoot = "opt/harness" },
		"oversized":      func(p *plan) { p.SetupCache.MaxBytes = maxSetupCacheBytes + 1 },
		"bad identity":   func(p *plan) { p.SetupCache.IdentitySHA256 = "abc" },
	} {
		broken := cachePlan()
		mutate(&broken)
		raw, _ := json.Marshal(broken)
		if _, err := decodePlan(raw); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestSetupCacheFetchVerifiesTheEntry(t *testing.T) {
	archive := []byte("harness-archive-bytes")
	for name, serve := range map[string]func(http.ResponseWriter){
		"hit": func(w http.ResponseWriter) {
			w.Header().Set("X-Loom-Content-SHA256", digestOf(archive))
			w.Header().Set("Content-Length", strconv.Itoa(len(archive)))
			_, _ = w.Write(archive)
		},
		"miss": func(w http.ResponseWriter) { http.Error(w, `{"detail":"harness_cache_miss"}`, http.StatusNotFound) },
		"corrupt": func(w http.ResponseWriter) {
			w.Header().Set("X-Loom-Content-SHA256", digestOf([]byte("other")))
			w.Header().Set("Content-Length", strconv.Itoa(len(archive)))
			_, _ = w.Write(archive)
		},
		"oversized": func(w http.ResponseWriter) {
			large := []byte(strings.Repeat("x", 2048))
			w.Header().Set("X-Loom-Content-SHA256", digestOf(large))
			w.Header().Set("Content-Length", strconv.Itoa(len(large)))
			_, _ = w.Write(large)
		},
	} {
		t.Run(name, func(t *testing.T) {
			broker := cacheBroker(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.URL.Path != "/internal/service-execution/harness-cache" ||
					r.Header.Get("Authorization") != "Bearer pod-token" || r.URL.RawQuery != "" {
					t.Errorf("unexpected request %s %s", r.Method, r.URL)
				}
				serve(w)
			})
			workspace := t.TempDir()
			err := broker.fetchSetupCache(context.Background(), cachePlan(), workspace)
			restored, readErr := os.ReadFile(setupCacheRestorePath(workspace))
			switch name {
			case "hit":
				if err != nil || readErr != nil || string(restored) != string(archive) {
					t.Fatalf("hit: %v %v %q", err, readErr, restored)
				}
			case "miss":
				if !errors.Is(err, errSetupCacheMiss) || readErr == nil {
					t.Fatalf("miss: %v", err)
				}
			default:
				if err == nil || readErr == nil {
					t.Fatalf("%s accepted", name)
				}
				if _, statErr := os.Stat(setupCacheRestorePath(workspace) + ".partial"); statErr == nil {
					t.Fatal("partial entry left behind")
				}
			}
		})
	}
}

func TestSetupCacheStoreUploadsOnlyAFreshArchive(t *testing.T) {
	var uploads [][]byte
	var digests []string
	broker := cacheBroker(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/internal/service-execution/harness-cache" {
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
		}
		body, _ := io.ReadAll(r.Body)
		uploads = append(uploads, body)
		digests = append(digests, r.Header.Get("X-Loom-Content-SHA256"))
		w.WriteHeader(http.StatusNoContent)
	})
	workspace := t.TempDir()
	if err := broker.storeSetupCache(context.Background(), cachePlan(), workspace); err != nil || len(uploads) != 0 {
		t.Fatalf("stored without an archive: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(setupCacheStorePath(workspace)), 0o700); err != nil {
		t.Fatal(err)
	}
	archive := []byte("fresh-install")
	if err := os.WriteFile(setupCacheStorePath(workspace), archive, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := broker.storeSetupCache(context.Background(), cachePlan(), workspace); err != nil {
		t.Fatal(err)
	}
	if len(uploads) != 1 || string(uploads[0]) != "fresh-install" || digests[0] != digestOf(archive) {
		t.Fatalf("uploads %q %v", uploads, digests)
	}
	oversized := cachePlan()
	oversized.SetupCache.MaxBytes = 4
	if err := broker.storeSetupCache(context.Background(), oversized, workspace); err == nil || len(uploads) != 1 {
		t.Fatal("oversized archive uploaded")
	}
}
