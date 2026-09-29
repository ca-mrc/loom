package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func testClient(t *testing.T) *http.Client {
	t.Helper()
	// macOS limits Unix socket path length, including temporary directory prefix.
	dir, err := os.MkdirTemp("/tmp", "loom-rpc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "rpc.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: (runtimeServer{1024, 3 * time.Second}).handler()}
	go server.Serve(listener)
	t.Cleanup(func() { server.Close() })
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", path)
	}}}
	t.Cleanup(client.CloseIdleConnections)
	if err := checkSocket(path); err != nil {
		t.Fatal(err)
	}
	return client
}

func runExec(t *testing.T, client *http.Client, req execRequest) (int, execResult) {
	t.Helper()
	data, _ := json.Marshal(req)
	response, err := client.Post("http://sandbox/exec", "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result execResult
	if response.StatusCode == 200 {
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
	}
	return response.StatusCode, result
}

func TestHealthIdentityIsStableForOneServerAndChangesOnRestart(t *testing.T) {
	identity := func(client *http.Client) string {
		t.Helper()
		response, err := client.Get("http://sandbox/health")
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var health struct {
			Ready      bool   `json:"ready"`
			InstanceID string `json:"instance_id"`
		}
		if err := json.NewDecoder(response.Body).Decode(&health); err != nil || !health.Ready || len(health.InstanceID) != 32 {
			t.Fatalf("invalid sandbox identity: %#v %v", health, err)
		}
		return health.InstanceID
	}
	client := testClient(t)
	first := identity(client)
	if identity(client) != first {
		t.Fatal("identity changed without a sandbox restart")
	}
	if identity(testClient(t)) == first {
		t.Fatal("replacement sandbox reused the old identity")
	}
}

func TestExecRealProcessAndBounds(t *testing.T) {
	c := testClient(t)
	uid := strconv.Itoa(os.Geteuid())
	status, result := runExec(t, c, execRequest{Argv: []string{"/bin/sh", "-c", `printf "%s:%s" "$VALUE" "$PWD"; printf failure >&2; exit 7`}, Cwd: "/", Env: map[string]string{"VALUE": "hello"}, User: &uid})
	if status != 200 || result.Code != 7 || string(result.Stdout) != "hello:/" || string(result.Stderr) != "failure" {
		t.Fatalf("unexpected execution: %d %#v", status, result)
	}
	other := strconv.Itoa(os.Geteuid() + 1)
	status, _ = runExec(t, c, execRequest{Argv: []string{"true"}, User: &other})
	if status != 400 {
		t.Fatal("accepted UID switch")
	}
	status, result = runExec(t, c, execRequest{Argv: []string{"/bin/sh", "-c", "yes x | head -c 11000000"}})
	if status != 200 || !result.Truncated || len(result.Stdout) != maxOutput {
		t.Fatal("output was not bounded")
	}
	status, _ = runExec(t, c, execRequest{Argv: []string{"true"}, Timeout: 4})
	if status != 400 {
		t.Fatal("accepted excessive timeout")
	}
}

func TestDeadlineAndDisconnectKillChildren(t *testing.T) {
	c := testClient(t)
	for _, disconnect := range []bool{false, true} {
		dir := t.TempDir()
		marker := filepath.Join(dir, "child-survived")
		command := "(sleep 0.4; touch '" + marker + "') & wait"
		if disconnect {
			data, _ := json.Marshal(execRequest{Argv: []string{"/bin/sh", "-c", command}})
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			req, _ := http.NewRequestWithContext(ctx, "POST", "http://sandbox/exec", bytes.NewReader(data))
			_, err := c.Do(req)
			cancel()
			if err == nil {
				t.Fatal("disconnect should cancel HTTP request")
			}
		} else {
			status, result := runExec(t, c, execRequest{Argv: []string{"/bin/sh", "-c", command}, Timeout: .1})
			if status != 200 || result.Code != 124 {
				t.Fatalf("timeout not returned: %d %#v", status, result)
			}
		}
		time.Sleep(500 * time.Millisecond)
		if _, err := os.Stat(marker); !os.IsNotExist(err) {
			t.Fatal("child survived cancelled exec")
		}
	}
}

func TestTransferBoundaries(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("openat transfer boundary requires Linux")
	}
	c := testClient(t)
	dir := t.TempDir()
	request := func(method, path, body string) (int, string) {
		t.Helper()
		req, _ := http.NewRequest(method, "http://sandbox/file", strings.NewReader(body))
		q := req.URL.Query()
		q.Set("path", path)
		req.URL.RawQuery = q.Encode()
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		data, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(data)
	}
	path := filepath.Join(dir, "nested", "data")
	if status, _ := request("PUT", path, "payload"); status != 204 {
		t.Fatal(status)
	}
	if status, data := request("GET", path, ""); status != 200 || data != "payload" {
		t.Fatalf("round trip: %d %q", status, data)
	}
	if status, _ := request("PUT", path, strings.Repeat("a", 1025)); status != 413 {
		t.Fatal("oversized upload accepted")
	}
	if _, data := request("GET", path, ""); data != "payload" {
		t.Fatal("failed upload changed existing file")
	}
	large := filepath.Join(dir, "large")
	if err := os.WriteFile(large, bytes.Repeat([]byte("x"), 1025), 0600); err != nil {
		t.Fatal(err)
	}
	if status, _ := request("GET", large, ""); status != 413 {
		t.Fatal("oversized download accepted")
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	parentLink := filepath.Join(dir, "parent-link")
	if err := os.Symlink(filepath.Join(dir, "nested"), parentLink); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{link, filepath.Join(parentLink, "data"), dir + "/../escape", "relative"} {
		for _, method := range []string{"GET", "PUT"} {
			if status, _ := request(method, bad, "untrusted"); status != 400 {
				t.Fatalf("accepted %s %s: %d", method, bad, status)
			}
		}
	}
}

func TestStopProcessesPreservesRPCAndRejectsHostProcess(t *testing.T) {
	c := testClient(t)
	if runtime.GOOS != "linux" || os.Getpid() != 1 {
		response, err := c.Post("http://sandbox/stop-processes", "application/json", nil)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != 409 {
			t.Fatal("cleanup allowed outside Linux PID 1")
		}
		return
	}
	marker := filepath.Join(t.TempDir(), "detached-child-survived")
	status, result := runExec(t, c, execRequest{Argv: []string{"/bin/sh", "-c", "(sleep 0.4; touch '" + marker + "') </dev/null >/dev/null 2>&1 &"}})
	if status != 200 || result.Code != 0 {
		t.Fatal("failed to start detached child")
	}
	response, err := c.Post("http://sandbox/stop-processes", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 204 {
		t.Fatalf("cleanup failed: %d", response.StatusCode)
	}
	time.Sleep(500 * time.Millisecond)
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatal("detached child survived cleanup")
	}
	status, result = runExec(t, c, execRequest{Argv: []string{"/bin/sh", "-c", "printf snapshot-ready"}})
	if status != 200 || string(result.Stdout) != "snapshot-ready" {
		t.Fatal("cleanup stopped RPC server")
	}
}

func TestStopProcessesFinishesAfterRequestCancellation(t *testing.T) {
	if runtime.GOOS != "linux" || os.Getpid() != 1 {
		t.Skip("cleanup requires an isolated Linux PID namespace with the runtime as PID 1")
	}
	handler := (runtimeServer{1024, 3 * time.Second}).handler()
	execute := func(command string) execResult {
		t.Helper()
		data, _ := json.Marshal(execRequest{Argv: []string{"/bin/sh", "-c", command}})
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("POST", "/exec", bytes.NewReader(data)))
		var result execResult
		if response.Code != http.StatusOK {
			t.Fatalf("exec failed: %d %s", response.Code, response.Body.String())
		}
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil || result.Code != 0 {
			t.Fatalf("exec failed: %#v %v", result, err)
		}
		return result
	}
	// The shell exits, leaving a live descendant adopted by this PID 1.
	result := execute("sleep 30 </dev/null >/dev/null 2>&1 & printf '%s' $!")
	pid, err := strconv.Atoi(string(result.Stdout))
	if err != nil || pid <= 1 {
		t.Fatalf("invalid detached child PID: %q", result.Stdout)
	}
	t.Cleanup(func() {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest("POST", "/stop-processes", nil))
	})
	processPath := filepath.Join("/proc", strconv.Itoa(pid))
	status, err := os.ReadFile(filepath.Join(processPath, "status"))
	if err != nil {
		t.Fatalf("child must still be alive before cancellation: %v", err)
	}
	if !strings.Contains(string(status), "PPid:\t1\n") || strings.Contains(string(status), "State:\tZ") {
		t.Fatalf("child was not a live adopted descendant: %s", status)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request := httptest.NewRequest("POST", "/stop-processes", nil).WithContext(ctx)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("accepted cleanup was interrupted by request cancellation: status=%d reason=%s",
			response.Code, response.Header().Get("X-Loom-Sandbox-Error"))
	}
	if _, err := os.Stat(processPath); !os.IsNotExist(err) {
		t.Fatalf("cleanup returned before the descendant was reaped: %v", err)
	}
	if result := execute("printf cleanup-complete"); string(result.Stdout) != "cleanup-complete" {
		t.Fatal("cleanup stopped the runtime execution endpoint")
	}
}

func TestExecRejectionReasonCodes(t *testing.T) {
	other := strconv.Itoa(os.Geteuid() + 1)
	cases := []struct{ name, body, reason string }{
		{"request", `{"argv":[],"unknown":"private-request"}`, "exec_request_invalid"},
		{"user", `{"argv":["true"],"user":"` + other + `"}`, "exec_user_mismatch"},
		{"timeout", `{"argv":["true"],"timeout_sec":4}`, "exec_timeout_invalid"},
		{"environment", `{"argv":["true"],"env":{"BAD=KEY":"private-value"}}`, "exec_environment_invalid"},
	}
	client := testClient(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			response, err := client.Post("http://sandbox/exec", "application/json", strings.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			body, err := io.ReadAll(response.Body)
			if err != nil {
				t.Fatal(err)
			}
			if response.StatusCode != http.StatusBadRequest || response.Header.Get("X-Loom-Sandbox-Error") != tc.reason {
				t.Fatalf("missing rejection reason: status=%d reason=%q", response.StatusCode, response.Header.Get("X-Loom-Sandbox-Error"))
			}
			if strings.Contains(string(body), "private-") {
				t.Fatal("request data leaked")
			}
		})
	}
}

func TestDownloadDescriptorMetadataAndBudget(t *testing.T) {
	c := testClient(t)
	path := filepath.Join(t.TempDir(), "reference")
	if err := os.WriteFile(path, []byte("payload"), 0751); err != nil {
		t.Fatal(err)
	}
	for _, budget := range []string{"7", "6", "0", "-1", "invalid"} {
		req, _ := http.NewRequest("GET", "http://sandbox/file", nil)
		query := req.URL.Query()
		query.Set("path", path)
		query.Set("max_bytes", budget)
		req.URL.RawQuery = query.Encode()
		response, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if budget == "7" {
			if response.StatusCode != 200 || string(data) != "payload" ||
				response.Header.Get("X-File-Unix-Mode") != "100751" ||
				response.Header.Get("X-File-UID") != fmt.Sprint(os.Getuid()) ||
				response.Header.Get("X-File-GID") != fmt.Sprint(os.Getgid()) {
				t.Fatalf("missing descriptor metadata: %v", response.Header)
			}
		} else if response.StatusCode != 400 && response.StatusCode != 413 {
			t.Fatalf("accepted reference budget %q: %d", budget, response.StatusCode)
		}
	}
}

func TestReadlinkReturnsLiteralTargetWithoutFollowing(t *testing.T) {
	c := testClient(t)
	dir := t.TempDir()
	for _, target := range []string{"python3.11", "/missing/image/python3.11"} {
		path := filepath.Join(dir, "alias")
		if err := os.Symlink(target, path); err != nil {
			t.Fatal(err)
		}
		req, _ := http.NewRequest("GET", "http://sandbox/readlink", nil)
		query := req.URL.Query()
		query.Set("path", path)
		req.URL.RawQuery = query.Encode()
		response, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || response.StatusCode != 200 || string(data) != target {
			t.Fatalf("literal alias read: status=%d data=%q error=%v", response.StatusCode, data, err)
		}
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReadlinkRejectsRegularAndUnsafePaths(t *testing.T) {
	c := testClient(t)
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "regular"), []byte("secret"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(dir, filepath.Join(dir, "parent-alias")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("regular", filepath.Join(dir, "leaf-alias")); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{dir, dir + "/regular", dir + "/missing", dir + "/parent-alias/leaf-alias", dir + "/../leaf-alias", "relative", dir + "//leaf-alias"} {
		req, _ := http.NewRequest("GET", "http://sandbox/readlink", nil)
		query := req.URL.Query()
		query.Set("path", path)
		req.URL.RawQuery = query.Encode()
		response, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || response.StatusCode != 400 || strings.Contains(string(data), "secret") {
			t.Fatalf("unsafe alias accepted: path=%q status=%d", path, response.StatusCode)
		}
	}
}
