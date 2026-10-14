package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
)

func TestOutputHTTPAttemptsCorrelateAndRedact(t *testing.T) {
	// Capture the production stderr sink, not the request or its credentials.
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	oldStderr := os.Stderr
	os.Stderr = writer
	defer func() { os.Stderr = oldStderr; _ = writer.Close(); _ = reader.Close() }()
	oldLog := log.Writer()
	log.SetOutput(writer)
	defer log.SetOutput(oldLog)
	body := []byte("PRIVATE-PAYLOAD")
	digest := sha256.Sum256(body)
	ids := []string{}
	count := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count++
		ids = append(ids, r.Header.Get("X-Loom-Upload-Request-Id"))
		content, _ := io.ReadAll(r.Body)
		if !bytes.Equal(content, body) {
			t.Error("changed body")
		}
		if count == 1 {
			http.Error(w, "PRIVATE-ERROR", 503)
			return
		}
		_ = json.NewEncoder(w).Encode(partReceipt{FileIndex: 2, PartNumber: 1, SizeBytes: int64(len(body)), SHA256: "sha256:" + hex.EncodeToString(digest[:])})
	}))
	defer server.Close()
	root, _ := url.Parse(server.URL + "/internal/service-execution")
	broker := &workloadBroker{root: root, client: server.Client(), identity: workloadIdentity{LeaseID: "10000000-0000-0000-0000-000000000001", Generation: 3, ExecutionRole: "attempt"}}
	grant := uploadGrant{UploadSessionID: "20000000-0000-0000-0000-000000000002", UploadToken: "SECRET-TOKEN"}
	err = retryOperation(context.Background(), func() error {
		_, err := broker.putPart(context.Background(), grant, 2, 1, body)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = writer.Close()
	raw, _ := io.ReadAll(reader)
	if len(ids) != 2 || ids[0] == "" || ids[0] == ids[1] {
		t.Fatalf("missing per-attempt correlation: %v", ids)
	}
	if strings.Contains(string(raw), "SECRET") || strings.Contains(string(raw), "PRIVATE") || strings.Contains(string(raw), server.URL) {
		t.Fatalf("unsanitized observations: %s", raw)
	}
	events := []map[string]any{}
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
		var event map[string]any
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	if len(events) != 4 {
		t.Fatalf("expected paired HTTP attempts, got %s", raw)
	}
	for i, status := range []float64{503, 200} {
		start, end := events[i*2], events[i*2+1]
		if start["event"] != "started" || end["event"] != "finished" || end["status_code"] != status || start["request_id"] != ids[i] || end["request_id"] != ids[i] {
			t.Fatalf("bad attempt: %v %v", start, end)
		}
		if start["declared_bytes"] != float64(len(body)) || end["duration_ns"].(float64) <= 0 {
			t.Fatal("missing boundaries")
		}
	}
}
