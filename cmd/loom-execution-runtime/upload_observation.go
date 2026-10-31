package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

var uploadProcessStart = time.Now()
var uploadProcessID = uploadObservationID()

func uploadObservationID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return ""
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", raw[:4], raw[4:6], raw[6:8], raw[8:10], raw[10:])
}

func observationUUID(value string) string {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return ""
	}
	if _, err := hex.DecodeString(strings.ReplaceAll(value, "-", "")); err != nil {
		return ""
	}
	return strings.ToLower(value)
}

type outputHTTPObservation struct {
	fields  map[string]any
	started time.Time
	status  int
}

func (b *workloadBroker) observeOutputHTTP(request *http.Request) *outputHTTPObservation {
	prefix := strings.TrimRight(b.root.Path, "/") + "/outputs/"
	if !strings.HasPrefix(request.URL.Path, prefix) {
		return nil
	}
	parts := strings.Split(strings.TrimPrefix(request.URL.Path, prefix), "/")
	operation, session := "", ""
	file, part := -1, -1
	switch {
	case len(parts) == 1 && parts[0] == "prepare":
		operation = "prepare"
	case len(parts) == 2 && parts[1] == "commit":
		operation, session = "commit", observationUUID(parts[0])
	case len(parts) == 2 && parts[1] == "small-files":
		operation, session = "small_files", observationUUID(parts[0])
	case len(parts) == 4 && parts[1] == "files" && parts[3] == "complete":
		operation, session = "complete", observationUUID(parts[0])
		file, _ = strconv.Atoi(parts[2])
	case len(parts) == 5 && parts[1] == "files" && parts[3] == "parts":
		operation, session = "part", observationUUID(parts[0])
		file, _ = strconv.Atoi(parts[2])
		part, _ = strconv.Atoi(parts[4])
	default:
		return nil
	}
	id := uploadObservationID()
	if id == "" {
		return nil
	}
	request.Header.Set("X-Loom-Upload-Request-Id", id)
	observation := &outputHTTPObservation{started: time.Now(), fields: map[string]any{
		"schema": "loom.upload-observation.v1", "phase": "source", "stage": "broker_http",
		"operation": operation, "request_id": id, "process_id": uploadProcessID,
		"lease_id": observationUUID(b.identity.LeaseID), "generation": b.identity.Generation,
		"declared_bytes": request.ContentLength,
	}}
	if session != "" {
		observation.fields["session_id"] = session
	}
	if file >= 0 {
		observation.fields["file_index"] = file
	}
	if part >= 0 {
		observation.fields["part_number"] = part
	}
	observation.emit("started")
	return observation
}

func (o *outputHTTPObservation) emit(event string) {
	o.fields["event"] = event
	o.fields["wall_time"] = time.Now().UTC().Format(time.RFC3339Nano)
	o.fields["monotonic_ns"] = time.Since(uploadProcessStart).Nanoseconds()
	if encoded, err := json.Marshal(o.fields); err == nil {
		_, _ = fmt.Fprintln(os.Stderr, string(encoded))
	}
}

func (o *outputHTTPObservation) finish(err error) {
	if o == nil {
		return
	}
	outcome := "success"
	if err != nil {
		outcome = "error"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		outcome = "timeout"
	}
	if errors.Is(err, context.Canceled) {
		outcome = "cancelled"
	}
	o.fields["outcome"] = outcome
	o.fields["status_code"] = o.status
	o.fields["duration_ns"] = time.Since(o.started).Nanoseconds()
	o.emit("finished")
}
