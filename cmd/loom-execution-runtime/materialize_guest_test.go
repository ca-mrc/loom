package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestGuestPayloadMaterializationIsBoundedAndExclusive(t *testing.T) {
	source := t.TempDir()
	if err := os.Mkdir(filepath.Join(source, "bin"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "bin/runtime"), []byte("binary"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "kernel"), []byte("kernel"), 0644); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "guest")
	if err := materializeGuestPayload(source, target, 12); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(target, "bin/runtime"))
	if err != nil || string(data) != "binary" {
		t.Fatalf("%s %v", data, err)
	}
	if info, err := os.Stat(filepath.Join(target, "bin/runtime")); err != nil || info.Mode().Perm() != 0555 {
		t.Fatalf("%v %v", info, err)
	}
	if err := materializeGuestPayload(source, target, 12); err == nil {
		t.Fatal("overwrote existing payload")
	}
	tooSmall := filepath.Join(t.TempDir(), "guest")
	if err := materializeGuestPayload(source, tooSmall, 11); err == nil {
		t.Fatal("ignored payload budget")
	}
	if _, err := os.Stat(tooSmall); !os.IsNotExist(err) {
		t.Fatalf("partial payload retained: %v", err)
	}
}

func TestGuestPayloadRefusesLinksAndPreservesForeignState(t *testing.T) {
	source := t.TempDir()
	foreign := t.TempDir()
	if err := os.WriteFile(filepath.Join(foreign, "keep"), []byte("foreign"), 0644); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "guest")
	if err := os.Symlink(foreign, target); err != nil {
		t.Fatal(err)
	}
	if err := materializeGuestPayload(source, target, 1024); err == nil {
		t.Fatal("accepted destination link")
	}
	if data, err := os.ReadFile(filepath.Join(foreign, "keep")); err != nil || string(data) != "foreign" {
		t.Fatalf("changed foreign state %s %v", data, err)
	}
	if err := os.Symlink(filepath.Join(foreign, "keep"), filepath.Join(source, "link")); err != nil {
		t.Fatal(err)
	}
	fresh := filepath.Join(t.TempDir(), "guest")
	if err := materializeGuestPayload(source, fresh, 1024); err == nil {
		t.Fatal("followed source link")
	}
	if _, err := os.Stat(fresh); !os.IsNotExist(err) {
		t.Fatalf("partial payload retained %v", err)
	}
}

func TestMaterializeGuestPayloadOnlyForExplicitGuestPlan(t *testing.T) {
	for _, guest := range []bool{false, true} {
		t.Run(fmt.Sprint(guest), func(t *testing.T) {
			raw := guestPlanPayload(t)
			if !guest {
				raw["execution_class_id"] = "linux-amd64-cpu-pod-v1"
				for _, s := range raw["sidecars"].([]any) {
					delete(s.(map[string]any), "guest_execution")
				}
			}
			executable, _ := os.Executable()
			binary, err := os.ReadFile(executable)
			if err != nil {
				t.Fatal(err)
			}
			digest := sha256.Sum256(binary)
			raw["runtime_binary_sha256"] = "sha256:" + hex.EncodeToString(digest[:])
			encoded, _ := json.Marshal(raw)
			dir := t.TempDir()
			source := filepath.Join(dir, "source")
			if err := os.Mkdir(source, 0755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(source, "kernel"), []byte("fixture kernel"), 0444); err != nil {
				t.Fatal(err)
			}
			sandbox := filepath.Join(dir, "sandbox-source")
			if err := os.WriteFile(sandbox, []byte("sandbox"), 0555); err != nil {
				t.Fatal(err)
			}
			destination := filepath.Join(dir, "payload")
			for _, role := range []string{"task-sandbox", "verifier-sandbox"} {
				if err := os.MkdirAll(filepath.Join(dir, "sandboxes", role), 0755); err != nil {
					t.Fatal(err)
				}
			}
			err = materialize([]string{"--encoded-plan", base64.RawURLEncoding.EncodeToString(encoded),
				"--runtime-dest", filepath.Join(dir, "runtime"), "--plan-dest", filepath.Join(dir, "plan.json"),
				"--sandbox-source", sandbox, "--sandbox-dest", filepath.Join(dir, "sandbox"),
				"--sandbox-root", filepath.Join(dir, "sandboxes"), "--guest-source", source, "--guest-dest", destination})
			if err != nil {
				t.Fatal(err)
			}
			got, err := os.ReadFile(filepath.Join(destination, "kernel"))
			if guest && (err != nil || string(got) != "fixture kernel") {
				t.Fatalf("guest payload missing %s %v", got, err)
			}
			if !guest && !os.IsNotExist(err) {
				t.Fatal("ordinary plan copied guest payload")
			}
		})
	}
}
