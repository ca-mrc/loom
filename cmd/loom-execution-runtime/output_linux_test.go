//go:build linux

package main

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestCaptureDeclaredFIFORejectsWithoutWaitingForWriter(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	path := filepath.Join(workspace, "answer.txt")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	p := completeOutputPlan(workspace)
	p.OutputDeclarations = p.OutputDeclarations[:1]
	result := resultManifest{Status: "succeeded"}
	done := make(chan error, 1)
	go func() { done <- captureDeclaredOutputs(p, workspace, output, &result) }()
	select {
	case err := <-done:
		if err == nil || result.Status != "artifact_upload_failed" || !result.PartialEvidence {
			t.Fatalf("FIFO did not produce bounded artifact failure: result=%#v err=%v", result, err)
		}
	case <-time.After(time.Second):
		// Release an unfixed blocking open before failing, without leaving a
		// goroutine or a FIFO writer behind in the test process.
		writer, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0)
		if err != nil {
			t.Fatal(err)
		}
		_ = writer.Close()
		<-done
		t.Fatal("artifact collection blocked opening a FIFO without a writer")
	}
}
