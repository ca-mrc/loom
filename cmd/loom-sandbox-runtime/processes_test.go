package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"testing"
	"time"
)

func startProcess(t *testing.T, client *http.Client, req execRequest) (int, processStatus, http.Header) {
	t.Helper()
	data, _ := json.Marshal(req)
	response, err := client.Post("http://sandbox/processes", "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var status processStatus
	if response.StatusCode == http.StatusCreated {
		if err := json.NewDecoder(response.Body).Decode(&status); err != nil {
			t.Fatal(err)
		}
	}
	return response.StatusCode, status, response.Header
}

func processStatusOf(t *testing.T, client *http.Client, id string, waitMS int) processStatus {
	t.Helper()
	response, err := client.Get(fmt.Sprintf("http://sandbox/processes/%s?wait_ms=%d", id, waitMS))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status %d", response.StatusCode)
	}
	var status processStatus
	if err := json.NewDecoder(response.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

// readAll follows offsets to EOF, optionally pausing between reads.
func readAll(t *testing.T, client *http.Client, id, stream string, pause time.Duration) []byte {
	t.Helper()
	var output []byte
	offset := int64(0)
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get(fmt.Sprintf("http://sandbox/processes/%s/output?stream=%s&offset=%d&wait_ms=500", id, stream, offset))
		if err != nil {
			t.Fatal(err)
		}
		chunk, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("output status %d", response.StatusCode)
		}
		output = append(output, chunk...)
		next, err := strconv.ParseInt(response.Header.Get("X-Loom-Next-Offset"), 10, 64)
		if err != nil || next != offset+int64(len(chunk)) {
			t.Fatalf("offset %d + %d != %s", offset, len(chunk), response.Header.Get("X-Loom-Next-Offset"))
		}
		offset = next
		if response.Header.Get("X-Loom-EOF") == "1" {
			return output
		}
		time.Sleep(pause)
	}
	t.Fatal("stream did not reach EOF")
	return nil
}

func TestProcessStreamsOutputAndExitStatus(t *testing.T) {
	client := testClient(t)
	code, started, _ := startProcess(t, client, execRequest{
		Argv: []string{"/bin/sh", "-c", "printf out; printf err >&2; exit 3"}, Cwd: "/",
	})
	if code != http.StatusCreated || started.ID == "" || started.PID <= 0 || !started.Running {
		t.Fatalf("start: %d %+v", code, started)
	}
	if got := readAll(t, client, started.ID, "stdout", 0); string(got) != "out" {
		t.Fatalf("stdout %q", got)
	}
	if got := readAll(t, client, started.ID, "stderr", 0); string(got) != "err" {
		t.Fatalf("stderr %q", got)
	}
	status := processStatusOf(t, client, started.ID, 5000)
	if status.Running || status.ExitCode == nil || *status.ExitCode != 3 || status.TimedOut || status.Killed {
		t.Fatalf("status %+v", status)
	}
}

func TestProcessBackpressureKeepsEveryByteInOrder(t *testing.T) {
	client := testClient(t)
	// More than one stream window, read slowly enough that the writer must wait.
	total := processStreamWindow + processStreamWindow/2
	_, started, _ := startProcess(t, client, execRequest{
		Argv: []string{"/bin/sh", "-c", fmt.Sprintf("head -c %d /dev/zero | tr '\\0' 'a'; printf END", total)}, Cwd: "/",
	})
	got := readAll(t, client, started.ID, "stdout", 5*time.Millisecond)
	if len(got) != total+3 || !bytes.HasSuffix(got, []byte("END")) || bytes.Count(got, []byte("a")) != total {
		t.Fatalf("got %d bytes", len(got))
	}
}

func TestProcessKillAndDeadline(t *testing.T) {
	client := testClient(t)
	_, running, _ := startProcess(t, client, execRequest{Argv: []string{"/bin/sh", "-c", "sleep 30 & sleep 30"}, Cwd: "/"})
	response, err := client.Post("http://sandbox/processes/"+running.ID+"/kill", "", nil)
	if err != nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("kill: %v %v", err, response)
	}
	response.Body.Close()
	killed := processStatusOf(t, client, running.ID, 5000)
	if killed.Running || !killed.Killed || killed.ExitCode == nil || *killed.ExitCode != 137 {
		t.Fatalf("killed %+v", killed)
	}
	readAll(t, client, running.ID, "stdout", 0) // the background child died with the group

	_, slow, _ := startProcess(t, client, execRequest{Argv: []string{"sleep", "30"}, Cwd: "/", Timeout: 0.3})
	timedOut := processStatusOf(t, client, slow.ID, 5000)
	if timedOut.Running || !timedOut.TimedOut || timedOut.ExitCode == nil || *timedOut.ExitCode != 124 {
		t.Fatalf("deadline %+v", timedOut)
	}
}

func TestProcessExitIsKnownWhileADescendantHoldsThePipes(t *testing.T) {
	client := testClient(t)
	_, started, _ := startProcess(t, client, execRequest{
		Argv: []string{"/bin/sh", "-c", "sleep 30 & echo parent-done"}, Cwd: "/",
	})
	status := processStatusOf(t, client, started.ID, 5000)
	if status.Running || status.ExitCode == nil || *status.ExitCode != 0 {
		t.Fatalf("status %+v", status)
	}
	began := time.Now()
	if got := readAll(t, client, started.ID, "stdout", 0); string(got) != "parent-done\n" {
		t.Fatalf("stdout %q", got)
	}
	if time.Since(began) > 5*time.Second {
		t.Fatal("descendant holding the pipes was not bounded")
	}
}

func TestProcessRequestRejectionsAndLifecycle(t *testing.T) {
	client := testClient(t)
	other := "65000"
	code, _, header := startProcess(t, client, execRequest{Argv: []string{"true"}, User: &other})
	if code != http.StatusBadRequest || header.Get("X-Loom-Sandbox-Error") != "exec_user_mismatch" {
		t.Fatalf("user: %d %s", code, header.Get("X-Loom-Sandbox-Error"))
	}
	code, _, header = startProcess(t, client, execRequest{Argv: []string{"true"}, Timeout: 99})
	if code != http.StatusBadRequest || header.Get("X-Loom-Sandbox-Error") != "exec_timeout_invalid" {
		t.Fatalf("timeout: %d", code)
	}
	if code, _, _ := startProcess(t, client, execRequest{}); code != http.StatusBadRequest {
		t.Fatalf("empty argv: %d", code)
	}

	_, running, _ := startProcess(t, client, execRequest{Argv: []string{"sleep", "30"}, Cwd: "/"})
	request, _ := http.NewRequest(http.MethodDelete, "http://sandbox/processes/"+running.ID, nil)
	response, _ := client.Do(request)
	response.Body.Close()
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("delete running: %d", response.StatusCode)
	}
	response, _ = client.Post("http://sandbox/processes/"+running.ID+"/kill", "", nil)
	response.Body.Close()
	processStatusOf(t, client, running.ID, 5000)
	response, _ = client.Do(request)
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("delete exited: %d", response.StatusCode)
	}
	response, _ = client.Get("http://sandbox/processes/" + running.ID)
	response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("deleted: %d", response.StatusCode)
	}
}

func TestProcessOutputOffsetsAreReleasedAfterReading(t *testing.T) {
	client := testClient(t)
	_, started, _ := startProcess(t, client, execRequest{Argv: []string{"printf", "abcdef"}, Cwd: "/"})
	processStatusOf(t, client, started.ID, 5000)
	get := func(offset int) (*http.Response, []byte) {
		response, err := client.Get(fmt.Sprintf("http://sandbox/processes/%s/output?stream=stdout&offset=%d&wait_ms=1000", started.ID, offset))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		return response, body
	}
	if _, body := get(0); string(body) != "abcdef" {
		t.Fatalf("first %q", body)
	}
	if response, body := get(3); response.StatusCode != http.StatusOK || string(body) != "def" {
		t.Fatalf("re-read later offset %d %q", response.StatusCode, body)
	}
	// Reading from offset 3 released bytes 0..2.
	if response, _ := get(0); response.StatusCode != http.StatusConflict ||
		response.Header.Get("X-Loom-Sandbox-Error") != "process_offset_unavailable" {
		t.Fatalf("released offset: %d", response.StatusCode)
	}
	if response, _ := get(99); response.StatusCode != http.StatusConflict {
		t.Fatalf("future offset: %d", response.StatusCode)
	}
}

func TestProcessLimitIsBounded(t *testing.T) {
	client := testClient(t)
	ids := []string{}
	for range maxManagedProcesses {
		code, started, _ := startProcess(t, client, execRequest{Argv: []string{"sleep", "30"}, Cwd: "/"})
		if code != http.StatusCreated {
			t.Fatalf("start: %d", code)
		}
		ids = append(ids, started.ID)
	}
	code, _, header := startProcess(t, client, execRequest{Argv: []string{"true"}, Cwd: "/"})
	if code != http.StatusTooManyRequests || header.Get("X-Loom-Sandbox-Error") != "process_limit_reached" {
		t.Fatalf("limit: %d", code)
	}
	for _, id := range ids {
		response, _ := client.Post("http://sandbox/processes/"+id+"/kill", "", nil)
		response.Body.Close()
	}
}
