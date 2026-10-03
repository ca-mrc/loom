//go:build linux

package main

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func directVerifierPlan(t *testing.T, workspace, script string) plan {
	t.Helper()
	path := filepath.Join(workspace, "verifier.sh")
	if err := os.WriteFile(path, []byte(script), 0o600); err != nil {
		t.Fatal(err)
	}
	p := testPlan(workspace, phase{
		Role: "agent", Argv: []string{"/bin/true"}, WorkingDirectory: workspace, TimeoutSeconds: 5,
	})
	p.RunAsUser, p.RunAsGroup = int64(os.Getuid()), int64(os.Getgid())
	p.VerifierExecution = "in_attempt"
	p.Verifier = &phase{
		Role: "verifier", Argv: []string{"/bin/sh", path}, WorkingDirectory: workspace, TimeoutSeconds: 5,
		Environment: map[string]string{"LOOM_VERIFIER_OUTPUT": filepath.Join(workspace, ".loom/verifier/output.json")},
	}
	p.OutputDeclarations = []outputDeclaration{{
		SourcePath: ".loom/verifier/output.json", RelativePath: "verifier/output.json", Kind: "verifier", Required: true,
	}}
	return p
}

func TestDirectVerifierPreparesReportDirectory(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	p := directVerifierPlan(t, workspace, `printf '{"rewards":{"passed":0}}' > "$LOOM_VERIFIER_OUTPUT"`)
	result, err := runPlan(context.Background(), p, workspace, output, nil)
	if err != nil {
		t.Fatalf("script could not write report: %v", err)
	}
	if err := captureDeclaredOutputs(p, workspace, output, &result); err != nil {
		t.Fatal(err)
	}
	if reward, ok := result.VerifierRewards["passed"]; !ok || reward != 0 || result.Status != "succeeded" {
		t.Fatalf("valid zero report was not captured: %#v", result)
	}
	for _, relative := range []string{".loom", ".loom/verifier", ".loom/verifier/output.json"} {
		info, err := os.Stat(filepath.Join(workspace, relative))
		if err != nil {
			t.Fatal(err)
		}
		owner := info.Sys().(*syscall.Stat_t)
		if owner.Uid != uint32(os.Getuid()) || owner.Gid != uint32(os.Getgid()) {
			t.Fatalf("report path changed runtime identity: %#v", owner)
		}
	}
}

func TestDirectVerifierCannotReuseStaleReport(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	p := directVerifierPlan(t, workspace, "exit 0")
	writeWorkspaceOutput(t, workspace, ".loom/verifier/output.json", `{"rewards":{"passed":1}}`)
	result, err := runPlan(context.Background(), p, workspace, output, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := captureDeclaredOutputs(p, workspace, output, &result); err == nil {
		t.Fatal("stale report accepted after verifier wrote nothing")
	}
	if result.Status != "verifier_error" || len(result.VerifierRewards) != 0 || !result.PartialEvidence {
		t.Fatalf("empty report did not fail validation: %#v", result)
	}
}

func TestDirectVerifierCannotCaptureUnpreparedReport(t *testing.T) {
	for _, blocked := range []string{"preparation-failed", "agent-failed"} {
		t.Run(blocked, func(t *testing.T) {
			workspace, output := t.TempDir(), t.TempDir()
			p := directVerifierPlan(t, workspace, "exit 0")
			writeWorkspaceOutput(t, workspace, ".loom/verifier/output.json", `{"rewards":{"passed":1}}`)
			if blocked == "agent-failed" {
				p.Main.Argv = []string{"/bin/false"}
			} else {
				directory := filepath.Join(workspace, ".loom/verifier")
				if err := os.Chmod(directory, 0o500); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Chmod(directory, 0o700) })
				// uid 0 without CAP_DAC_OVERRIDE also observes this failure in
				// the Docker lane. Ordinary privileged root may still write.
				probe := filepath.Join(directory, "probe")
				if err := os.WriteFile(probe, nil, 0o600); err == nil {
					_ = os.Remove(probe)
					t.Skip("runtime identity can bypass directory write permission")
				}
			}
			result, err := runPlan(context.Background(), p, workspace, output, nil)
			if err == nil {
				t.Fatal("expected execution failure")
			}
			if err := captureDeclaredOutputs(p, workspace, output, &result); err == nil {
				t.Fatal("unprepared report accepted after execution failed before verifier launch")
			}
			if len(result.VerifierRewards) != 0 {
				t.Fatalf("stale reward reused: %#v", result.VerifierRewards)
			}
			if len(result.Outputs) != 1 || result.Outputs[0].State != "missing" {
				t.Fatalf("stale report was published: %#v", result.Outputs)
			}
		})
	}
}

func TestDirectVerifierRetainsAuthoredPartialReport(t *testing.T) {
	workspace, output := t.TempDir(), t.TempDir()
	p := directVerifierPlan(t, workspace, `printf '{"rewards":{"passed":0}}' > "$LOOM_VERIFIER_OUTPUT"; exit 7`)
	result, err := runPlan(context.Background(), p, workspace, output, nil)
	if err == nil || result.Status != "verifier_error" {
		t.Fatal("expected verifier failure")
	}
	if err := captureDeclaredOutputs(p, workspace, output, &result); err != nil {
		t.Fatal(err)
	}
	if reward, ok := result.VerifierRewards["passed"]; !ok || reward != 0 || !result.PartialEvidence {
		t.Fatalf("valid partial report lost: %#v", result)
	}
}

func TestDirectVerifierRejectsPlantedReportPaths(t *testing.T) {
	for _, planted := range []string{"workspace-parent-link", "loom-link", "directory-link", "report-link", "loom-file", "report-directory", "report-fifo", "outside-report"} {
		t.Run(planted, func(t *testing.T) {
			workspace, output, protected := t.TempDir(), t.TempDir(), t.TempDir()
			if planted == "workspace-parent-link" {
				alias := filepath.Join(t.TempDir(), "alias")
				if err := os.Symlink(filepath.Dir(workspace), alias); err != nil {
					t.Fatal(err)
				}
				workspace = filepath.Join(alias, filepath.Base(workspace))
			}
			p := directVerifierPlan(t, workspace, `printf entered > entered; printf '{"rewards":{"passed":0}}' > "$LOOM_VERIFIER_OUTPUT"`)
			report := p.Verifier.Environment["LOOM_VERIFIER_OUTPUT"]
			writeWorkspaceOutput(t, protected, "verifier/output.json", "protected")
			var err error
			switch planted {
			case "workspace-parent-link":
			case "loom-link":
				err = os.Symlink(protected, filepath.Join(workspace, ".loom"))
			case "directory-link":
				if err := os.Mkdir(filepath.Join(workspace, ".loom"), 0o700); err != nil {
					t.Fatal(err)
				}
				err = os.Symlink(filepath.Join(protected, "verifier"), filepath.Dir(report))
			case "loom-file":
				err = os.WriteFile(filepath.Join(workspace, ".loom"), nil, 0o600)
			case "outside-report":
				p.Verifier.Environment["LOOM_VERIFIER_OUTPUT"] = filepath.Join(protected, "verifier/output.json")
			default:
				if err := os.MkdirAll(filepath.Dir(report), 0o700); err != nil {
					t.Fatal(err)
				}
				switch planted {
				case "report-link":
					err = os.Symlink(filepath.Join(protected, "verifier/output.json"), report)
				case "report-directory":
					err = os.Mkdir(report, 0o700)
				case "report-fifo":
					err = syscall.Mkfifo(report, 0o600)
					// Bound the unfixed script's blocking FIFO write for RED.
					p.Verifier.TimeoutSeconds = 1
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			result, err := runPlan(context.Background(), p, workspace, output, nil)
			if err == nil || result.Status != "verifier_error" || len(result.Phases) != 2 || result.Phases[1].ExitCode != -1 {
				t.Fatalf("unsafe report path was not rejected before launch: %#v, %v", result, err)
			}
			if _, err := os.Stat(filepath.Join(workspace, "entered")); !os.IsNotExist(err) {
				t.Fatalf("script entered despite unsafe report: %v", err)
			}
			body, err := os.ReadFile(filepath.Join(protected, "verifier/output.json"))
			if err != nil || string(body) != "protected" {
				t.Fatalf("preparation modified protected data: %q, %v", body, err)
			}
		})
	}
}

func TestDirectVerifierReportReplacementDoesNotTruncateHardlink(t *testing.T) {
	workspace, output, protected := t.TempDir(), t.TempDir(), t.TempDir()
	p := directVerifierPlan(t, workspace, `printf '{"rewards":{"passed":0}}' > "$LOOM_VERIFIER_OUTPUT"`)
	report := p.Verifier.Environment["LOOM_VERIFIER_OUTPUT"]
	writeWorkspaceOutput(t, protected, "original", "protected")
	if err := os.MkdirAll(filepath.Dir(report), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(protected, "original"), report); err != nil {
		t.Fatal(err)
	}
	result, err := runPlan(context.Background(), p, workspace, output, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := captureDeclaredOutputs(p, workspace, output, &result); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(protected, "original"))
	if err != nil || string(body) != "protected" {
		t.Fatalf("report creation truncated hardlink target: %q, %v", body, err)
	}
}
