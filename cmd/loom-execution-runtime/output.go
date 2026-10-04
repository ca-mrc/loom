package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
)

const maxVerifierResultBytes int64 = 16 * 1024 * 1024

func captureDeclaredOutputs(
	p plan,
	workspace, outputRoot string,
	result *resultManifest,
) error {
	result.Outputs = make([]outputEvidence, 0, len(p.OutputDeclarations))
	var total int64
	var firstError error
	unpreparedVerifier := !result.verifierReportPrepared && (declaresVerifierReport(p.Main) ||
		(p.Verifier != nil && declaresVerifierReport(*p.Verifier)))
	for _, declaration := range p.OutputDeclarations {
		evidence := outputEvidence{
			SourcePath: declaration.SourcePath, RelativePath: declaration.RelativePath,
			Kind: declaration.Kind, Required: declaration.Required, State: "missing",
		}
		if declaration.Kind == "verifier" && unpreparedVerifier {
			// Preparation can fail while an old report is still readable, or
			// execution can stop before the verifier phase. Never publish that
			// file as this attempt's authored grading evidence.
			if firstError == nil {
				firstError = fmt.Errorf("verifier report was not prepared for this execution")
			}
			if result.Status == "succeeded" {
				result.Status = "verifier_error"
				result.PartialEvidence = true
			}
			result.Outputs = append(result.Outputs, evidence)
			continue
		}
		size, digest, err := copyWorkspaceOutput(
			workspace,
			outputRoot,
			declaration,
			p.MaxArtifactBytes-total,
		)
		if err == nil {
			total += size
			evidence.State = "captured"
			evidence.SizeBytes = &size
			evidence.SHA256 = digest
		} else if declaration.Required || !os.IsNotExist(err) {
			if firstError == nil {
				firstError = fmt.Errorf("capture %s: %w", declaration.SourcePath, err)
			}
			if result.Status == "succeeded" {
				result.Status = captureFailureStatus(declaration.Kind, os.IsNotExist(err))
				result.PartialEvidence = true
			}
		}
		result.Outputs = append(result.Outputs, evidence)
	}
	// Evaluation is independent evidence, including for partial failed attempts.
	// A missing trajectory must not discard an already captured numeric reward.
	rewards, err := verifierRewards(outputRoot, result.Outputs)
	if err != nil {
		if firstError == nil {
			firstError = err
		}
		if result.Status == "succeeded" {
			result.Status = "verifier_error"
			result.PartialEvidence = true
		}
	} else {
		result.VerifierRewards = rewards
	}
	return firstError
}

func captureFailureStatus(kind string, missing bool) string {
	if missing {
		switch kind {
		case "trajectory", "agent_native", "checkpoint":
			return "trajectory_flush_failed"
		case "verifier":
			return "verifier_error"
		default:
			return "missing_required_artifacts"
		}
	}
	return "artifact_upload_failed"
}

func copyWorkspaceOutput(
	workspace, outputRoot string,
	declaration outputDeclaration,
	remaining int64,
) (int64, string, error) {
	if remaining < 0 {
		return 0, "", fmt.Errorf("artifact bundle exceeds runtime bound")
	}
	sourcePath := filepath.Join(workspace, filepath.FromSlash(declaration.SourcePath))
	if !isWithin(workspace, sourcePath) {
		return 0, "", fmt.Errorf("workspace output escapes root")
	}
	resolved, err := filepath.EvalSymlinks(sourcePath)
	if err != nil {
		return 0, "", err
	}
	resolvedWorkspace, err := filepath.EvalSymlinks(workspace)
	if err != nil {
		return 0, "", err
	}
	if !isWithin(resolvedWorkspace, resolved) {
		return 0, "", fmt.Errorf("workspace output resolves outside root")
	}
	source, info, err := openRegularOutputFile(sourcePath)
	if err != nil {
		return 0, "", err
	}
	defer source.Close()
	if info.Size() > remaining {
		return 0, "", fmt.Errorf("artifact bundle exceeds runtime bound")
	}
	destinationPath := filepath.Join(outputRoot, filepath.FromSlash(declaration.RelativePath))
	if !isWithin(outputRoot, destinationPath) {
		return 0, "", fmt.Errorf("bundle output escapes root")
	}
	if err := secureOutputParent(outputRoot, filepath.Dir(destinationPath)); err != nil {
		return 0, "", err
	}
	temporaryPath := destinationPath + ".tmp"
	destination, err := os.OpenFile(
		temporaryPath,
		os.O_CREATE|os.O_EXCL|os.O_WRONLY,
		0o600,
	)
	if err != nil {
		return 0, "", err
	}
	committed := false
	defer func() {
		_ = destination.Close()
		if !committed {
			_ = os.Remove(temporaryPath)
		}
	}()
	hash := sha256.New()
	written, err := io.Copy(io.MultiWriter(destination, hash), io.LimitReader(source, remaining+1))
	if err != nil {
		return 0, "", err
	}
	if written > remaining || written != info.Size() {
		return 0, "", fmt.Errorf("workspace output size changed or exceeds runtime bound")
	}
	if err := destination.Sync(); err != nil {
		return 0, "", err
	}
	if err := destination.Close(); err != nil {
		return 0, "", err
	}
	if err := os.Rename(temporaryPath, destinationPath); err != nil {
		return 0, "", err
	}
	committed = true
	return written, "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

func secureOutputParent(root, parent string) error {
	if parent != root && !isWithin(root, parent) {
		return fmt.Errorf("bundle directory escapes output root")
	}
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return err
	}
	resolved, err := filepath.EvalSymlinks(parent)
	if err != nil {
		return err
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return err
	}
	if resolved != resolvedRoot && !isWithin(resolvedRoot, resolved) {
		return fmt.Errorf("bundle directory resolves outside output root")
	}
	return nil
}

func verifierRewards(outputRoot string, outputs []outputEvidence) (map[string]float64, error) {
	for _, item := range outputs {
		// Verifier diagnostics share this kind but are not scoring documents.
		// Use the canonical path also consumed by the materializer, independent
		// of declaration order or rewards-like fields in an exception artifact.
		if item.Kind != "verifier" || item.RelativePath != "verifier/output.json" || item.State != "captured" {
			continue
		}
		if item.SizeBytes == nil || *item.SizeBytes > maxVerifierResultBytes {
			return nil, fmt.Errorf("verifier result exceeds parse bound")
		}
		payload, err := os.ReadFile(filepath.Join(outputRoot, filepath.FromSlash(item.RelativePath)))
		if err != nil {
			return nil, fmt.Errorf("read verifier result: %w", err)
		}
		var document struct {
			Rewards map[string]float64 `json:"rewards"`
		}
		if err := json.Unmarshal(payload, &document); err != nil {
			return nil, fmt.Errorf("decode verifier result: %w", err)
		}
		if len(document.Rewards) == 0 {
			return nil, fmt.Errorf("verifier result has no rewards")
		}
		for name, value := range document.Rewards {
			if name == "" || len(name) > 256 || math.IsNaN(value) || math.IsInf(value, 0) {
				return nil, fmt.Errorf("verifier result has invalid reward")
			}
		}
		return document.Rewards, nil
	}
	return nil, nil
}
