//go:build linux

package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func workspaceTransferPath(t *testing.T) string {
	t.Helper()
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		t.Fatal(err)
	}
	path := "/tmp/loom-workspace-" + hex.EncodeToString(token[:]) + ".tar"
	t.Cleanup(func() { os.Remove(path) })
	return path
}

func TestWorkspaceArchiveBudgetPreservesOrdinaryFileLimit(t *testing.T) {
	path := workspaceTransferPath(t)
	if err := os.WriteFile(path, []byte("archive-data"), 0600); err != nil {
		t.Fatal(err)
	}
	s := httptest.NewServer((runtimeServer{maxTransfer: 8, maxWorkspaceArchive: 16}).handler())
	defer s.Close()
	for _, endpoint := range []string{"/file", "/workspace-archive"} {
		for _, method := range []string{"GET", "PUT"} {
			req, _ := http.NewRequest(method, s.URL+endpoint+"?path="+url.QueryEscape(path), strings.NewReader("archive-data"))
			response, err := s.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			data, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil {
				t.Fatal(err)
			}
			if endpoint == "/file" {
				if response.StatusCode != 413 || response.Header.Get("X-Loom-Sandbox-Error") != "file_too_large" || response.Header.Get("X-Loom-File-Size") != "12" || response.Header.Get("X-Loom-File-Limit") != "8" || bytes.Contains(data, []byte(path)) {
					t.Fatalf("missing safe limit diagnostic: %d %v %q", response.StatusCode, response.Header, data)
				}
			} else if (method == "GET" && (response.StatusCode != 200 || string(data) != "archive-data")) || (method == "PUT" && response.StatusCode != 204) {
				t.Fatalf("archive within separate budget failed: %s %d %q", method, response.StatusCode, data)
			}
		}
	}
	// The trusted archive budget is also a hard cap, even for chunked bodies.
	for _, declared := range []bool{false, true} {
		req := httptest.NewRequest("PUT", "/workspace-archive?path="+url.QueryEscape(path), strings.NewReader(strings.Repeat("x", 17)))
		if !declared {
			req.ContentLength = -1
		}
		w := httptest.NewRecorder()
		(runtimeServer{maxTransfer: 8, maxWorkspaceArchive: 16}).handler().ServeHTTP(w, req)
		if w.Code != 413 {
			t.Fatalf("oversized archive accepted: %d", w.Code)
		}
		data, _ := os.ReadFile(path)
		if string(data) != "archive-data" {
			t.Fatal("failed upload replaced old archive")
		}
	}
}

func TestWorkspaceArchiveRejectsUnsafeTargetsAndInterruptedUploads(t *testing.T) {
	path := workspaceTransferPath(t)
	old := filepath.Join(t.TempDir(), "private-original")
	if err := os.WriteFile(old, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(old, path); err != nil {
		t.Fatal(err)
	}
	s := runtimeServer{maxTransfer: 8, maxWorkspaceArchive: 16}
	for _, target := range []string{path, old, "/tmp/loom-workspace-" + strings.Repeat("a", 32) + ".tar/../private"} {
		for _, method := range []string{"GET", "PUT"} {
			w := httptest.NewRecorder()
			s.handler().ServeHTTP(w, httptest.NewRequest(method, "/workspace-archive?path="+url.QueryEscape(target), strings.NewReader("new")))
			if w.Code != 400 || w.Header().Get("X-Loom-Sandbox-Error") != "file_invalid_target" || strings.Contains(w.Body.String(), "private") {
				t.Fatalf("unsafe archive accepted or leaked: %d %v %q", w.Code, w.Header(), w.Body.String())
			}
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest("PUT", "/workspace-archive?path="+url.QueryEscape(path), nil)
	req.Body = io.NopCloser(io.MultiReader(strings.NewReader("partial"), transferReadFailure{}))
	req.ContentLength = 12
	w := httptest.NewRecorder()
	s.handler().ServeHTTP(w, req)
	data, _ := os.ReadFile(path)
	if w.Code != 400 || string(data) != "old" {
		t.Fatalf("interrupted upload published: %d %q", w.Code, data)
	}
	data, _ = os.ReadFile(old)
	if string(data) != "old" {
		t.Fatal("symlink target changed")
	}
}

type transferReadFailure struct{}

func (transferReadFailure) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

type changingTransferWriter struct {
	*httptest.ResponseRecorder
	change func()
}

func (w *changingTransferWriter) Write(p []byte) (int, error) {
	n, err := w.ResponseRecorder.Write(p)
	if w.change != nil {
		change := w.change
		w.change = nil
		change()
	}
	return n, err
}

func TestWorkspaceDownloadAbortsShortOrChangedDescriptor(t *testing.T) {
	for _, mutation := range []string{"tiny", "changed!", "grown-data"} {
		t.Run(mutation, func(t *testing.T) {
			path := workspaceTransferPath(t)
			if err := os.WriteFile(path, []byte("original"), 0600); err != nil {
				t.Fatal(err)
			}
			w := &changingTransferWriter{ResponseRecorder: httptest.NewRecorder(), change: func() {
				if err := os.WriteFile(path, []byte(mutation), 0600); err != nil {
					t.Fatal(err)
				}
				// Make equal-length edits deterministic even on coarse timestamp filesystems.
				if err := os.Chtimes(path, time.Unix(1, 0), time.Unix(1, 0)); err != nil {
					t.Fatal(err)
				}
			}}
			defer func() {
				if recovered := recover(); recovered != http.ErrAbortHandler {
					t.Fatalf("changed download not aborted: %v", recovered)
				}
				if w.Body.Len() >= 8 {
					t.Fatal("changed file sent its complete declared body")
				}
			}()
			(runtimeServer{maxTransfer: 8, maxWorkspaceArchive: 16}).handler().ServeHTTP(w,
				httptest.NewRequest("GET", "/workspace-archive?path="+url.QueryEscape(path), nil))
		})
	}
}
