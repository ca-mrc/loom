package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func containerExecRPC(t *testing.T, server runtimeServer, document any) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	server.handler().ServeHTTP(response, httptest.NewRequest("POST", "/container-exec", bytes.NewReader(body)))
	return response
}

func TestContainerExecRPCOnlyExposesRootGuestController(t *testing.T) {
	for _, identity := range []*guestTaskIdentity{nil, {uid: 0, gid: 0, home: "/root"}, {uid: 1000, gid: 1000}} {
		var requests atomic.Int32
		client := containerExecTestClient(t, containerExecHandler(t, containerExecFrame(1, []byte("123")),
			containerExecInspection(75), &requests))
		server := runtimeServer{maxTimeout: 5 * time.Second, identity: identity, containerExec: client}
		response := containerExecRPC(t, server, map[string]any{"container": containerExecTestContainer,
			"argv": []string{"true"}, "timeout_sec": 5})
		if identity != nil && identity.uid != 0 {
			if response.Code != http.StatusNotFound || requests.Load() != 0 {
				t.Fatal("non-root guest exposed container controls")
			}
			continue
		}
		var result execResult
		if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &result) != nil ||
			result.Code != 75 || string(result.Stdout) != "123" || requests.Load() != 3 {
			t.Fatal("root controller lost the private control result")
		}
	}
	response := containerExecRPC(t, runtimeServer{maxTimeout: 5 * time.Second}, map[string]any{})
	if response.Code != http.StatusNotFound {
		t.Fatal("ordinary host sandbox exposed container controls")
	}
}

func TestContainerExecRPCRejectsInvalidControlsBeforeDocker(t *testing.T) {
	var requests atomic.Int32
	client := containerExecTestClient(t, func(http.ResponseWriter, *http.Request) { requests.Add(1) })
	for _, document := range []map[string]any{
		{"container": "unowned/name", "argv": []string{"true"}, "timeout_sec": 5},
		{"container": containerExecTestContainer, "argv": []string{}, "timeout_sec": 5},
		{"container": containerExecTestContainer, "argv": []string{"private\x00command"}, "timeout_sec": 5},
		{"container": containerExecTestContainer, "argv": []string{"true"}, "timeout_sec": 6},
		{"container": containerExecTestContainer, "argv": []string{"true"}, "timeout_sec": 5, "user": "1000"},
	} {
		response := containerExecRPC(t, runtimeServer{maxTimeout: 5 * time.Second, containerExec: client}, document)
		if response.Code != http.StatusBadRequest || requests.Load() != 0 || strings.Contains(response.Body.String(), "private") {
			t.Fatal("invalid control reached Docker or exposed its input")
		}
	}
}

func TestContainerExecRPCPreservesDeadlineAndPrivateFailures(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		var requests atomic.Int32
		client := containerExecTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			_, _ = io.Copy(io.Discard, r.Body)
			if deadline {
				<-r.Context().Done()
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("private-daemon-error"))
		})
		response := containerExecRPC(t, runtimeServer{maxTimeout: 5 * time.Second, containerExec: client},
			map[string]any{"container": containerExecTestContainer, "argv": []string{"private-command"}, "timeout_sec": .02})
		if requests.Load() != 1 || strings.Contains(response.Body.String(), "private") {
			t.Fatal("control retried or exposed private data")
		}
		if !deadline {
			if response.Code != http.StatusBadGateway || response.Header().Get("X-Loom-Sandbox-Error") != "container_exec_failed" {
				t.Fatal("daemon failure was hidden")
			}
			continue
		}
		var result execResult
		if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &result) != nil || result.Code != 124 ||
			result.Duration < .015 || result.Duration > 1 || len(result.Stdout) != 0 || len(result.Stderr) != 0 {
			t.Fatal("control did not retain its aggregate deadline result")
		}
	}
}
