package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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

func TestCommitSmallOutputsNegotiatesBatchAndRetainsMultipart(t *testing.T) {
	for _, mixed := range []bool{false, true} {
		t.Run(fmt.Sprint("mixed=", mixed), func(t *testing.T) {
			root := t.TempDir()
			for i := 0; i < 11; i++ {
				name := fmt.Sprintf("%02d.stdout", i)
				body := []byte{}
				if i == 0 {
					name, body = "result.json", []byte("{}")
				}
				if mixed && i == 10 {
					body = []byte(strings.Repeat("x", 32))
				}
				if err := os.WriteFile(filepath.Join(root, name), body, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			tokenFile := filepath.Join(t.TempDir(), "token")
			if err := os.WriteFile(tokenFile, []byte("pod-token"), 0o600); err != nil {
				t.Fatal(err)
			}
			calls, batches, parts, completes := 0, 0, 0, 0
			var inventory []outputFile
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.Header.Get("Authorization") != "Bearer pod-token" {
					t.Error("missing Pod authorization")
				}
				switch {
				case strings.HasSuffix(r.URL.Path, "/prepare"):
					var prepared outputPrepare
					if err := json.NewDecoder(r.Body).Decode(&prepared); err != nil {
						t.Fatal(err)
					}
					inventory = prepared.Files
					plans := []uploadPlanFile{}
					for index, file := range inventory {
						plans = append(plans, uploadPlanFile{index, file.RelativePath})
					}
					_ = json.NewEncoder(w).Encode(uploadGrant{UploadSessionID: "session", UploadToken: "upload", Files: plans,
						SmallFileBatch: &smallFileBatchLimits{MaxFiles: 16, MaxTotalBytes: 1024, MaxFileBytes: 8}})
				case strings.HasSuffix(r.URL.Path, "/small-files"):
					batches++
					var batch struct {
						Files []smallOutputFile `json:"files"`
					}
					if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
						t.Fatal(err)
					}
					if r.Header.Get("X-Loom-Upload-Token") != "upload" {
						t.Error("missing upload token")
					}
					verified := []verifiedOutputFile{}
					for _, item := range batch.Files {
						digest := sha256.Sum256(item.Content)
						sha := "sha256:" + hex.EncodeToString(digest[:])
						if sha != inventory[item.FileIndex].SHA256 {
							t.Error("content drift")
						}
						verified = append(verified, verifiedOutputFile{item.FileIndex, int64(len(item.Content)), sha})
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"files": verified})
				case strings.Contains(r.URL.Path, "/parts/"):
					parts++
					body, _ := io.ReadAll(r.Body)
					digest := sha256.Sum256(body)
					segments := strings.Split(r.URL.Path, "/")
					index, _ := strconv.Atoi(segments[len(segments)-3])
					_ = json.NewEncoder(w).Encode(partReceipt{index, 1, int64(len(body)), "sha256:" + hex.EncodeToString(digest[:])})
				case strings.HasSuffix(r.URL.Path, "/complete"):
					completes++
					_, _ = w.Write([]byte("{}"))
				case strings.HasSuffix(r.URL.Path, "/commit"):
					_ = json.NewEncoder(w).Encode(outputCommitEvidence{UploadSessionID: "session", ManifestSHA256: "sha256:" + strings.Repeat("1", 64), CommittedMarkerSHA256: "sha256:" + strings.Repeat("2", 64)})
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			endpoint, _ := url.Parse(server.URL + "/internal/service-execution")
			broker := &workloadBroker{root: endpoint, podTokenFile: tokenFile, identity: workloadIdentity{LeaseID: "lease", Generation: 1, ExecutionRole: "attempt"}, client: server.Client()}
			if _, err := broker.commitOutputs(context.Background(), root, "request"); err != nil {
				t.Fatal(err)
			}
			if batches != 1 {
				t.Fatalf("batches=%d", batches)
			}
			if !mixed && (calls != 3 || parts != 0 || completes != 0) {
				t.Fatalf("small-file requests=%d parts=%d completes=%d", calls, parts, completes)
			}
			if mixed && (calls != 5 || parts != 1 || completes != 1) {
				t.Fatalf("mixed requests=%d parts=%d completes=%d", calls, parts, completes)
			}
		})
	}
}
