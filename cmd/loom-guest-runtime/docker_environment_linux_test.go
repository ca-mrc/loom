package main

import (
	"strings"
	"testing"
)

func TestDockerDaemonDoesNotInheritHostRuntimeOrTaskEnvironment(t *testing.T) {
	for _, name := range []string{"XDG_RUNTIME_DIR", "DOCKER_HOST", "HOME", "TMPDIR", "LD_PRELOAD"} {
		t.Setenv(name, "/host-only/runtime")
	}
	environment := strings.Join(dockerDaemonEnvironment(), "\n")
	if strings.Contains(environment, "/host-only") || strings.Contains(environment, "XDG_RUNTIME_DIR=") || strings.Contains(environment, "LD_PRELOAD=") {
		t.Fatalf("daemon inherited ambient runtime state: %q", environment)
	}
	if !strings.Contains(environment, "HOME=/root\nTMPDIR=/tmp") {
		t.Fatal("daemon lacks guest-owned state directories")
	}
	task := strings.Join(guestEnvironment([]string{"TASK_MODE=authored", "XDG_RUNTIME_DIR=/task/runtime"}), "\n")
	if !strings.Contains(task, "TASK_MODE=authored") || !strings.Contains(task, "XDG_RUNTIME_DIR=/task/runtime") {
		t.Fatal("daemon isolation changed authored task environment")
	}
}
