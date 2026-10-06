package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var (
	digestImage       = regexp.MustCompile(`^.+@sha256:[0-9a-f]{64}$`)
	sha256Value       = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
	candidate         = regexp.MustCompile(`^[0-9a-f]{40}$`)
	execClass         = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,79}$`)
	roleName          = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	keyID             = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)
	envName           = regexp.MustCompile(`^[A-Z_][A-Z0-9_]{0,127}$`)
	secretEnv         = regexp.MustCompile(`(?:KEY|TOKEN|SECRET|PASSWORD|CREDENTIAL|KUBECONFIG)`)
	workspacePath     = regexp.MustCompile(`^/workspace(?:/[-A-Za-z0-9._]+)*$`)
	probePath         = regexp.MustCompile(`^/[ -~]*$`)
	materializationID = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
)

type resources struct {
	CPUMillis           int64 `json:"cpu_millis"`
	MemoryMiB           int64 `json:"memory_mib"`
	EphemeralStorageMiB int64 `json:"ephemeral_storage_mib"`
}

type phase struct {
	Role             string            `json:"role"`
	Argv             []string          `json:"argv"`
	WorkingDirectory string            `json:"working_directory"`
	TimeoutSeconds   int64             `json:"timeout_seconds"`
	Environment      map[string]string `json:"environment"`
}

type probe struct {
	InitialDelaySeconds int64    `json:"initial_delay_seconds,omitempty"`
	Kind                string   `json:"kind"`
	TimeoutSeconds      int64    `json:"timeout_seconds"`
	PeriodSeconds       int64    `json:"period_seconds"`
	FailureThreshold    int64    `json:"failure_threshold"`
	Port                *int64   `json:"port"`
	Path                *string  `json:"path"`
	Argv                []string `json:"argv"`
}

type guestExecution struct {
	SchemaVersion string   `json:"schema_version"`
	Runtime       string   `json:"runtime"`
	Capabilities  []string `json:"capabilities"`
}

type sidecar struct {
	GuestExecution     *guestExecution   `json:"guest_execution,omitempty"`
	TaskFixture        bool              `json:"task_fixture,omitempty"`
	TaskImageComponent *string           `json:"task_image_component,omitempty"`
	Hostname           *string           `json:"hostname,omitempty"`
	Identity           *sandboxIdentity  `json:"identity,omitempty"`
	PrivateSandbox     bool              `json:"private_sandbox,omitempty"`
	RoleName           string            `json:"role_name"`
	ImageRef           string            `json:"image_ref"`
	Argv               []string          `json:"argv"`
	Environment        map[string]string `json:"environment"`
	Resources          resources         `json:"resources"`
	StartupProbe       probe             `json:"startup_probe"`
	ReadinessProbe     probe             `json:"readiness_probe"`
	DependsOn          []string          `json:"depends_on"`
}

type sandboxIdentity struct {
	RunAsUser  *int64 `json:"run_as_user"`
	RunAsGroup *int64 `json:"run_as_group"`
	Home       string `json:"home"`
}

type taskInput struct {
	SchemaVersion  string `json:"schema_version"`
	ManifestSHA256 string `json:"manifest_sha256"`
	FileCount      int    `json:"file_count"`
	TotalBytes     int64  `json:"total_bytes"`
}

type outputDeclaration struct {
	SourcePath   string `json:"source_path"`
	RelativePath string `json:"relative_path"`
	Kind         string `json:"kind"`
	Required     bool   `json:"required"`
}

type imageAdmissionStatement struct {
	SchemaVersion                string    `json:"schema_version"`
	ImageRef                     string    `json:"image_ref"`
	Platform                     string    `json:"platform"`
	SBOMSHA256                   string    `json:"sbom_sha256"`
	ProvenanceSHA256             string    `json:"provenance_sha256"`
	VulnerabilityReportSHA256    string    `json:"vulnerability_report_sha256"`
	PolicySHA256                 string    `json:"policy_sha256"`
	HighestVulnerabilitySeverity string    `json:"highest_vulnerability_severity"`
	IssuedAt                     time.Time `json:"issued_at"`
	ExpiresAt                    time.Time `json:"expires_at"`
}

type signedImageAdmission struct {
	Statement       imageAdmissionStatement `json:"statement"`
	SigningKeyID    string                  `json:"signing_key_id"`
	SignatureBase64 string                  `json:"signature_base64"`
}

type executionImageAdmission struct {
	SchemaVersion string                 `json:"schema_version"`
	Admissions    []signedImageAdmission `json:"admissions"`
}

type executionResourceRequests struct {
	Controller      *resources `json:"controller,omitempty"`
	TaskSandbox     *resources `json:"task_sandbox,omitempty"`
	VerifierSandbox *resources `json:"verifier_sandbox,omitempty"`
}

type nodeResourceAllocation struct {
	Policy        string    `json:"policy"`
	TargetID      string    `json:"target_id"`
	UsableNode    resources `json:"usable_node"`
	BaselineSlots int64     `json:"baseline_slots"`
	DeclaredTask  resources `json:"declared_task"`
}

type plan struct {
	NodeResourceAllocation *nodeResourceAllocation `json:"node_resource_allocation,omitempty"`
	TaskEgress             *storedTaskEgress       `json:"task_egress,omitempty"`
	// Install sources reachable only while a setup phase runs (#2310).
	SetupEgress *webAllowlist `json:"setup_egress,omitempty"`
	// A cacheable harness install keyed by the Gateway (#2310).
	SetupCache                 *setupCache                `json:"setup_cache,omitempty"`
	EffectiveNetworkPolicy     json.RawMessage            `json:"effective_network_policy,omitempty"`
	SchemaVersion              string                     `json:"schema_version"`
	CandidateSHA               string                     `json:"candidate_sha"`
	TaskRevisionSHA256         string                     `json:"task_revision_sha256"`
	CommandIdentitySHA256      string                     `json:"command_identity_sha256"`
	ExecutionRole              string                     `json:"execution_role"`
	ExecutionClassID           string                     `json:"execution_class_id"`
	Composition                string                     `json:"composition"`
	TaskImageRef               string                     `json:"task_image_ref"`
	TaskImageMaterializationID *string                    `json:"task_image_materialization_id,omitempty"`
	AgentImageRef              *string                    `json:"agent_image_ref,omitempty"`
	RuntimeImageRef            string                     `json:"runtime_image_ref"`
	RuntimeBinarySHA256        string                     `json:"runtime_binary_sha256"`
	ImageAdmission             executionImageAdmission    `json:"image_admission"`
	RunAsUser                  int64                      `json:"run_as_user"`
	RunAsGroup                 int64                      `json:"run_as_group"`
	FSGroup                    int64                      `json:"fs_group"`
	TaskResources              resources                  `json:"task_resources"`
	ControllerResources        *resources                 `json:"controller_resources,omitempty"`
	ResourceRequests           *executionResourceRequests `json:"resource_requests,omitempty"`
	WorkspaceMiB               int64                      `json:"workspace_mib"`
	RuntimeVolumeMiB           int64                      `json:"runtime_volume_mib"`
	TerminationGraceSec        int64                      `json:"termination_grace_seconds"`
	Setup                      []phase                    `json:"setup"`
	Main                       phase                      `json:"main"`
	VerifierExecution          string                     `json:"verifier_execution"`
	Verifier                   *phase                     `json:"verifier"`
	VerifierAfterAgentTimeout  bool                       `json:"verifier_after_agent_timeout,omitempty"`
	InPlaceVerifier            bool                       `json:"in_place_verifier,omitempty"`
	Sidecars                   []sidecar                  `json:"sidecars"`
	MaxLogBytesPerStream       int64                      `json:"max_log_bytes_per_stream"`
	MaxArtifactBytes           int64                      `json:"max_artifact_bytes"`
	TaskInput                  *taskInput                 `json:"task_input"`
	HandoffInput               *taskInput                 `json:"handoff_input,omitempty"`
	OutputDeclarations         []outputDeclaration        `json:"output_declarations"`
	RuntimeContractSHA256      string                     `json:"-"`
}

func loadPlan(path string) (plan, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return plan{}, fmt.Errorf("read plan: %w", err)
	}
	return decodePlan(payload)
}

func decodePlan(payload []byte) (plan, error) {
	if len(payload) == 0 || len(payload) > 256*1024 {
		return plan{}, fmt.Errorf("plan size is outside 1..262144 bytes")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var result plan
	if err := decoder.Decode(&result); err != nil {
		return plan{}, fmt.Errorf("decode plan: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return plan{}, fmt.Errorf("plan contains trailing JSON")
	}
	if err := result.validate(); err != nil {
		return plan{}, err
	}
	digest := sha256.Sum256(payload)
	result.RuntimeContractSHA256 = "sha256:" + hex.EncodeToString(digest[:])
	return result, nil
}

func (p plan) residentPrivateRoles() map[string]bool {
	if p.ExecutionRole == "verifier" {
		return map[string]bool{"verifier-sandbox": true}
	}
	if p.InPlaceVerifier || p.VerifierExecution == "separate_execution" {
		return map[string]bool{"task-sandbox": true}
	}
	return map[string]bool{"task-sandbox": true, "verifier-sandbox": true}
}

func (p plan) privateSandboxesMatch() bool {
	expected := p.residentPrivateRoles()
	found := map[string]bool{}
	for _, item := range p.Sidecars {
		if item.PrivateSandbox {
			found[item.RoleName] = true
		}
	}
	if len(found) != len(expected) {
		return false
	}
	for name := range expected {
		if !found[name] {
			return false
		}
	}
	return true
}

func (p plan) validate() error {
	if err := p.validateGuestExecution(); err != nil {
		return err
	}
	if p.SetupEgress != nil {
		if len(p.Setup) == 0 {
			return fmt.Errorf("setup egress requires a setup phase")
		}
		if err := p.SetupEgress.validate(); err != nil {
			return err
		}
	}
	if p.SetupCache != nil {
		if len(p.Setup) == 0 {
			return fmt.Errorf("a setup cache requires a setup phase")
		}
		if err := p.SetupCache.validate(); err != nil {
			return err
		}
	}
	if p.TaskEgress != nil || p.SetupEgress != nil {
		declared := false
		for _, output := range p.OutputDeclarations {
			if output == taskEgressOutput {
				declared = true
			}
		}
		if !declared {
			return fmt.Errorf("task egress requires its immutable diagnostic output declaration")
		}
		if p.TaskEgress != nil {
			if err := p.TaskEgress.validate(); err != nil {
				return err
			}
		}
	}
	if err := p.validateEffectiveNetworkPolicy(); err != nil {
		return err
	}
	if p.SchemaVersion != "loom.execution-runtime-plan.v1" || !candidate.MatchString(p.CandidateSHA) {
		return fmt.Errorf("invalid schema version or candidate SHA")
	}
	if !sha256Value.MatchString(p.TaskRevisionSHA256) || !sha256Value.MatchString(p.RuntimeBinarySHA256) || !sha256Value.MatchString(p.CommandIdentitySHA256) {
		return fmt.Errorf("invalid content digest")
	}
	if p.ExecutionRole != "attempt" && p.ExecutionRole != "verifier" {
		return fmt.Errorf("invalid execution role")
	}
	if !execClass.MatchString(p.ExecutionClassID) {
		return fmt.Errorf("invalid execution class")
	}
	if !digestImage.MatchString(p.TaskImageRef) || !digestImage.MatchString(p.RuntimeImageRef) {
		return fmt.Errorf("images must be digest-pinned")
	}
	if p.AgentImageRef != nil && !digestImage.MatchString(*p.AgentImageRef) {
		return fmt.Errorf("agent image must be digest-pinned")
	}
	if p.TaskImageMaterializationID != nil &&
		(!materializationID.MatchString(*p.TaskImageMaterializationID) ||
			*p.TaskImageMaterializationID == "00000000-0000-0000-0000-000000000000" ||
			p.AgentImageRef == nil || p.Composition != "init_payload") {
		return fmt.Errorf("prepared task images require a nonzero materialization UUID and separate trusted attempt controller")
	}
	if p.RunAsUser <= 0 || p.RunAsUser > 2_147_483_647 ||
		p.RunAsGroup <= 0 || p.RunAsGroup > 2_147_483_647 ||
		p.FSGroup <= 0 || p.FSGroup > 2_147_483_647 {
		return fmt.Errorf("runtime user and group must be non-root")
	}
	if p.Composition != "precomposed" && p.Composition != "init_payload" {
		return fmt.Errorf("invalid runtime composition")
	}
	if err := p.TaskResources.validate(); err != nil {
		return fmt.Errorf("invalid task resources: %w", err)
	}
	if p.WorkspaceMiB <= 0 || p.WorkspaceMiB > 1_048_576 ||
		p.RuntimeVolumeMiB <= 0 || p.RuntimeVolumeMiB > 4096 ||
		p.TerminationGraceSec <= 0 || p.TerminationGraceSec > 300 {
		return fmt.Errorf("invalid volume or termination bound")
	}
	if p.MaxLogBytesPerStream <= 0 || p.MaxLogBytesPerStream > 100*1024*1024 ||
		p.MaxArtifactBytes <= 0 || p.MaxArtifactBytes > 10*1024*1024*1024 {
		return fmt.Errorf("invalid output bounds")
	}
	if p.TaskInput != nil && (p.TaskInput.SchemaVersion != "loom.runtime-task-input.v1" ||
		!sha256Value.MatchString(p.TaskInput.ManifestSHA256) || p.TaskInput.FileCount <= 0 ||
		p.TaskInput.FileCount > 10_000 || p.TaskInput.TotalBytes < 0 ||
		p.TaskInput.TotalBytes > 10*1024*1024*1024) {
		return fmt.Errorf("invalid task input binding")
	}
	if p.HandoffInput != nil && (p.HandoffInput.SchemaVersion != "loom.runtime-handoff-input.v1" ||
		!sha256Value.MatchString(p.HandoffInput.ManifestSHA256) || p.HandoffInput.FileCount <= 0 ||
		p.HandoffInput.FileCount > 10_000 || p.HandoffInput.TotalBytes < 0 ||
		p.HandoffInput.TotalBytes > 10*1024*1024*1024 ||
		p.ExecutionRole != "verifier" || p.TaskInput == nil) {
		return fmt.Errorf("invalid workspace handoff binding")
	}
	if len(p.Setup) > 32 || len(p.Sidecars) > 32 {
		return fmt.Errorf("runtime plan exceeds phase or sidecar bounds")
	}
	if len(p.OutputDeclarations) > 10_000 {
		return fmt.Errorf("runtime plan exceeds output declaration bounds")
	}
	sourcePaths := map[string]bool{}
	bundlePaths := map[string]bool{}
	for _, item := range p.OutputDeclarations {
		if !validRelativeOutputPath(item.SourcePath) ||
			!validRelativeOutputPath(item.RelativePath) ||
			sourcePaths[item.SourcePath] || bundlePaths[item.RelativePath] ||
			!validOutputKind(item.Kind) || !validBundleNamespace(item.RelativePath) {
			return fmt.Errorf("invalid runtime output declaration")
		}
		sourcePaths[item.SourcePath] = true
		bundlePaths[item.RelativePath] = true
	}
	for _, item := range append(append([]phase{}, p.Setup...), p.Main) {
		if err := item.validate(); err != nil {
			return err
		}
	}
	for _, item := range p.Setup {
		if item.Role != "setup" {
			return fmt.Errorf("setup phase has wrong role")
		}
	}
	if p.ExecutionRole == "attempt" {
		if p.Main.Role != "agent" {
			return fmt.Errorf("attempt main phase has wrong role")
		}
		if p.VerifierExecution == "in_attempt" {
			if p.Verifier == nil || p.Verifier.Role != "verifier" {
				return fmt.Errorf("in-attempt verifier is missing")
			}
			if err := p.Verifier.validate(); err != nil {
				return err
			}
		} else if p.VerifierExecution != "separate_execution" && p.VerifierExecution != "skipped" {
			return fmt.Errorf("invalid verifier execution")
		} else if p.Verifier != nil {
			return fmt.Errorf("external verifier cannot have an in-attempt phase")
		}
	} else if p.Main.Role != "verifier" || p.VerifierExecution != "skipped" || p.Verifier != nil {
		return fmt.Errorf("invalid verifier execution unit")
	}
	known := map[string]bool{}
	fixtureCount := 0
	for _, item := range p.Sidecars {
		if item.TaskFixture {
			fixtureCount++
		}
		if item.PrivateSandbox && p.AgentImageRef == nil {
			return fmt.Errorf("private sandboxes require an agent image reference")
		}
		if err := item.validate(known); err != nil {
			return err
		}
		known[item.RoleName] = true
	}
	if fixtureCount > 0 && (fixtureCount != 1 || p.TaskImageMaterializationID == nil ||
		p.AgentImageRef == nil || p.ExecutionRole != "attempt" || p.Composition != "init_payload" ||
		!p.privateSandboxesMatch() || len(p.Sidecars) != 1+len(p.residentPrivateRoles()) || !p.Sidecars[0].TaskFixture) {
		return fmt.Errorf("one prepared fixture requires an isolated attempt controller and both sandboxes")
	}
	if p.VerifierAfterAgentTimeout && (p.ExecutionRole != "attempt" ||
		p.Composition != "init_payload" || p.AgentImageRef == nil ||
		p.VerifierExecution != "in_attempt" || !p.privateSandboxesMatch()) {
		return fmt.Errorf("timeout verification requires an isolated attempt controller and in-attempt verifier")
	}
	if p.ControllerResources != nil || p.ResourceRequests != nil {
		if p.ControllerResources != nil {
			if err := p.ControllerResources.validate(); err != nil {
				return fmt.Errorf("invalid controller resources: %w", err)
			}
		}
		if p.AgentImageRef == nil || (p.ExecutionRole != "attempt" && p.ExecutionRole != "verifier") || p.Composition != "init_payload" {
			return fmt.Errorf("controller resources require an isolated attempt controller")
		}
		for _, item := range p.Sidecars {
			if item.PrivateSandbox && item.Resources != p.TaskResources {
				return fmt.Errorf("controller sizing must preserve task and verifier resources")
			}
		}
		if !p.privateSandboxesMatch() {
			return fmt.Errorf("controller resources require an isolated attempt controller")
		}
		expectedControllerStorage := p.TaskResources.EphemeralStorageMiB
		for _, sidecar := range p.Sidecars {
			if sidecar.GuestExecution != nil {
				expectedControllerStorage += p.RuntimeVolumeMiB
				break
			}
		}
		if p.NodeResourceAllocation == nil && p.ControllerResources != nil && p.ControllerResources.EphemeralStorageMiB != expectedControllerStorage {
			return fmt.Errorf("controller sizing must preserve task-derived storage")
		}
		if p.ResourceRequests != nil {
			limits := p.TaskResources
			if p.ControllerResources != nil {
				limits = *p.ControllerResources
			}
			configured := false
			for _, pair := range []struct {
				request *resources
				limit   resources
			}{
				{p.ResourceRequests.Controller, limits},
				{p.ResourceRequests.TaskSandbox, p.TaskResources},
				{p.ResourceRequests.VerifierSandbox, p.TaskResources},
			} {
				if pair.request == nil {
					continue
				}
				configured = true
				if err := pair.request.validate(); err != nil {
					return fmt.Errorf("invalid resource requests: %w", err)
				}
				if pair.request.CPUMillis > pair.limit.CPUMillis || pair.request.MemoryMiB > pair.limit.MemoryMiB || pair.request.EphemeralStorageMiB > pair.limit.EphemeralStorageMiB {
					return fmt.Errorf("resource requests exceed hard limits")
				}
			}
			if !configured {
				return fmt.Errorf("resource requests must configure at least one container")
			}
		}
	}
	if allocation := p.NodeResourceAllocation; allocation != nil {
		if allocation.Policy != "node-share-v1" || len(allocation.TargetID) == 0 || len(allocation.TargetID) > 80 {
			return fmt.Errorf("invalid node resource allocation policy or target")
		}
		if err := allocation.UsableNode.validate(); err != nil {
			return err
		}
		if err := allocation.DeclaredTask.validate(); err != nil {
			return err
		}
		slots := allocation.UsableNode.CPUMillis / 1000
		if slots < 1 {
			slots = 1
		}
		if allocation.BaselineSlots != slots {
			return fmt.Errorf("node allocation slot count does not match usable CPU")
		}
		minimum := allocation.DeclaredTask
		if p.TaskResources.CPUMillis < minimum.CPUMillis || p.TaskResources.MemoryMiB < minimum.MemoryMiB || p.TaskResources.EphemeralStorageMiB < minimum.EphemeralStorageMiB {
			return fmt.Errorf("node allocation cannot reduce declared task requirements")
		}
		if p.ResourceRequests != nil {
			controller := p.TaskResources
			if p.ControllerResources != nil {
				controller = *p.ControllerResources
			}
			for _, pair := range []struct {
				request *resources
				limit   resources
			}{
				{p.ResourceRequests.Controller, controller},
				{p.ResourceRequests.TaskSandbox, p.TaskResources},
				{p.ResourceRequests.VerifierSandbox, p.TaskResources},
			} {
				if pair.request != nil && pair.request.MemoryMiB != pair.limit.MemoryMiB {
					return fmt.Errorf("node allocation memory requests must equal limits")
				}
			}
		}
	}
	// The authenticated lease carries the Control Plane's task-image authorization.
	// Only that prepared task image and matching private sandboxes are exempt from
	// platform publication; runtime, controller and other sidecars remain covered.
	requiredImages := []string{p.RuntimeImageRef}
	if p.TaskImageMaterializationID == nil {
		requiredImages = append(requiredImages, p.TaskImageRef)
	}
	if p.AgentImageRef != nil {
		requiredImages = append(requiredImages, *p.AgentImageRef)
	}
	for _, item := range p.Sidecars {
		if p.TaskImageMaterializationID != nil && (item.TaskFixture || (item.PrivateSandbox && item.ImageRef == p.TaskImageRef)) {
			continue
		}
		requiredImages = append(requiredImages, item.ImageRef)
	}
	if err := p.ImageAdmission.validate(requiredImages, time.Now().UTC()); err != nil {
		return err
	}
	return nil
}

func validRelativeOutputPath(value string) bool {
	if value == "" || len(value) > 4096 || strings.Contains(value, "\\") || strings.ContainsRune(value, '\x00') {
		return false
	}
	clean := filepath.ToSlash(filepath.Clean(value))
	return clean == value && clean != "." && !strings.HasPrefix(clean, "/") &&
		!strings.HasPrefix(clean, "../") && !strings.Contains(clean, "/../")
}

func validOutputKind(value string) bool {
	switch value {
	case "task_artifact", "trajectory", "agent_native", "verifier", "usage", "diagnostic", "checkpoint":
		return true
	default:
		return false
	}
}

func validBundleNamespace(value string) bool {
	namespace := strings.SplitN(value, "/", 2)[0]
	switch namespace {
	case "artifacts", "trajectory", "agent", "verifier", "accounting", "diagnostics", "checkpoints":
		return true
	default:
		return false
	}
}

func (bundle executionImageAdmission) validate(requiredImages []string, now time.Time) error {
	if bundle.SchemaVersion != "loom.execution-image-admission.v1" ||
		len(bundle.Admissions) == 0 || len(bundle.Admissions) > 34 {
		return fmt.Errorf("invalid image admission bundle")
	}
	required := map[string]bool{}
	for _, imageRef := range requiredImages {
		required[imageRef] = true
	}
	actual := map[string]bool{}
	for _, admission := range bundle.Admissions {
		statement := admission.Statement
		if actual[statement.ImageRef] || !required[statement.ImageRef] ||
			statement.SchemaVersion != "loom.image-admission-statement.v1" ||
			statement.Platform != "linux/x86_64" || !digestImage.MatchString(statement.ImageRef) ||
			!sha256Value.MatchString(statement.SBOMSHA256) ||
			!sha256Value.MatchString(statement.ProvenanceSHA256) ||
			!sha256Value.MatchString(statement.VulnerabilityReportSHA256) ||
			!sha256Value.MatchString(statement.PolicySHA256) ||
			!keyID.MatchString(admission.SigningKeyID) {
			return fmt.Errorf("invalid image admission identity")
		}
		if statement.HighestVulnerabilitySeverity == "critical" {
			return fmt.Errorf("image admission vulnerability policy failed")
		}
		if statement.HighestVulnerabilitySeverity != "none" &&
			statement.HighestVulnerabilitySeverity != "negligible" &&
			statement.HighestVulnerabilitySeverity != "low" &&
			statement.HighestVulnerabilitySeverity != "medium" &&
			statement.HighestVulnerabilitySeverity != "high" &&
			statement.HighestVulnerabilitySeverity != "unknown" {
			return fmt.Errorf("invalid image admission severity")
		}
		signature, err := base64.StdEncoding.Strict().DecodeString(admission.SignatureBase64)
		if err != nil || len(signature) != 64 {
			return fmt.Errorf("invalid image admission signature encoding")
		}
		// ExpiresAt records the scanner/policy decision's evidence horizon.  It
		// is not an online credential and must not turn an already published,
		// digest-pinned runtime profile into a human-renewed availability gate.
		// Issuance must still be well formed and must not come from the future.
		if statement.IssuedAt.IsZero() || statement.ExpiresAt.IsZero() ||
			!statement.ExpiresAt.After(statement.IssuedAt) ||
			statement.IssuedAt.After(now.Add(5*time.Minute)) {
			return fmt.Errorf("invalid image admission lifetime")
		}
		actual[statement.ImageRef] = true
	}
	if len(actual) != len(required) {
		return fmt.Errorf("image admission coverage does not match runtime images")
	}
	return nil
}

func (p phase) validate() error {
	if p.Role != "setup" && p.Role != "agent" && p.Role != "verifier" {
		return fmt.Errorf("invalid phase role")
	}
	if len(p.Argv) == 0 || len(p.Argv) > 128 || p.TimeoutSeconds <= 0 ||
		p.TimeoutSeconds > 86_400 || filepath.Clean(p.WorkingDirectory) != p.WorkingDirectory ||
		(p.WorkingDirectory != "/app" && !workspacePath.MatchString(p.WorkingDirectory)) {
		return fmt.Errorf("invalid %s phase", p.Role)
	}
	argvBytes := 0
	for _, item := range p.Argv {
		argvBytes += len(item)
		if item == "" || strings.ContainsRune(item, '\x00') || len(item) > 4096 {
			return fmt.Errorf("invalid %s argv", p.Role)
		}
	}
	if argvBytes > 32_768 {
		return fmt.Errorf("invalid %s argv", p.Role)
	}
	if len(p.Environment) > 64 {
		return fmt.Errorf("invalid %s environment", p.Role)
	}
	for name, value := range p.Environment {
		if !envName.MatchString(name) || secretEnv.MatchString(name) || strings.ContainsRune(value, '\x00') || len(value) > 4096 {
			return fmt.Errorf("invalid %s environment", p.Role)
		}
	}
	return nil
}

func (s sidecar) validate(known map[string]bool) error {
	if s.TaskFixture {
		if s.TaskImageComponent == nil || !strings.HasPrefix(*s.TaskImageComponent, "sidecar:") {
			return fmt.Errorf("fixture requires its prepared sidecar component")
		}
		name := strings.TrimPrefix(*s.TaskImageComponent, "sidecar:")
		if !fixtureName.MatchString(name) || fixtureReserved[name] || s.RoleName != "fixture-"+name {
			return fmt.Errorf("fixture role must match its prepared component")
		}
		if s.Hostname == nil || !validFixtureHostname(*s.Hostname) {
			return fmt.Errorf("fixture requires a non-reserved DNS hostname")
		}
		if s.PrivateSandbox || s.Identity != nil || len(s.DependsOn) != 0 || len(s.Environment) != 0 {
			return fmt.Errorf("fixture cannot share a sandbox identity or trusted sidecar contract")
		}
	} else if s.TaskImageComponent != nil || s.Hostname != nil || strings.HasPrefix(s.RoleName, "fixture-") {
		return fmt.Errorf("fixture metadata and roles require explicit fixture isolation")
	}
	if s.Identity != nil {
		i := s.Identity
		if !s.PrivateSandbox || i.RunAsUser == nil || i.RunAsGroup == nil || *i.RunAsUser < 0 || *i.RunAsUser > 2_147_483_647 || *i.RunAsGroup < 0 || *i.RunAsGroup > 2_147_483_647 {
			return fmt.Errorf("invalid private sandbox identity")
		}
		if _, present := s.Environment["HOME"]; present {
			return fmt.Errorf("sandbox HOME must use identity")
		}
		if len(i.Home) > 4096 || !regexp.MustCompile(`^/(?:[-A-Za-z0-9._]+/)*[-A-Za-z0-9._]+$`).MatchString(i.Home) || filepath.Clean(i.Home) != i.Home {
			return fmt.Errorf("invalid sandbox HOME")
		}
		for _, protected := range []string{"/proc", "/sys", "/dev", "/run", "/loom", "/tests", "/verifier", "/solution"} {
			if i.Home == protected || strings.HasPrefix(i.Home, protected+"/") {
				return fmt.Errorf("protected sandbox HOME")
			}
		}
	}
	if s.PrivateSandbox != (s.RoleName == "task-sandbox" || s.RoleName == "verifier-sandbox") {
		return fmt.Errorf("sandbox roles require private mounts")
	}
	if !roleName.MatchString(s.RoleName) ||
		s.RoleName == "execution" || s.RoleName == "runtime-materializer" ||
		s.RoleName == "setup" || s.RoleName == "agent" || s.RoleName == "verifier" {
		return fmt.Errorf("invalid sidecar role")
	}
	if known[s.RoleName] || !digestImage.MatchString(s.ImageRef) || len(s.Argv) == 0 || len(s.Argv) > 128 {
		return fmt.Errorf("invalid sidecar identity")
	}
	if err := s.Resources.validate(); err != nil {
		return fmt.Errorf("invalid sidecar resources")
	}
	argvBytes := 0
	for _, item := range s.Argv {
		argvBytes += len(item)
		if item == "" || strings.ContainsRune(item, '\x00') || len(item) > 4096 {
			return fmt.Errorf("invalid sidecar argv")
		}
	}
	if argvBytes > 32_768 || len(s.DependsOn) > 32 || len(s.Environment) > 64 {
		return fmt.Errorf("invalid sidecar bounds")
	}
	for _, dependency := range s.DependsOn {
		if !known[dependency] {
			return fmt.Errorf("sidecar dependency is not ordered")
		}
	}
	for name, value := range s.Environment {
		if !envName.MatchString(name) || secretEnv.MatchString(name) || strings.ContainsRune(value, '\x00') || len(value) > 4096 {
			return fmt.Errorf("invalid sidecar environment")
		}
	}
	if err := s.StartupProbe.validate(); err != nil {
		return err
	}
	return s.ReadinessProbe.validate()
}

func (p probe) validate() error {
	if p.InitialDelaySeconds < 0 || p.InitialDelaySeconds > 300 || p.TimeoutSeconds <= 0 || p.TimeoutSeconds > 30 ||
		p.PeriodSeconds <= 0 || p.PeriodSeconds > 60 ||
		p.FailureThreshold <= 0 || p.FailureThreshold > 300 || len(p.Argv) > 32 {
		return fmt.Errorf("invalid probe bounds")
	}
	switch p.Kind {
	case "http":
		if p.Port == nil || *p.Port <= 0 || *p.Port > 65_535 ||
			p.Path == nil || len(*p.Path) > 1024 || !probePath.MatchString(*p.Path) || len(p.Argv) != 0 {
			return fmt.Errorf("invalid http probe")
		}
	case "tcp":
		if p.Port == nil || *p.Port <= 0 || *p.Port > 65_535 || p.Path != nil || len(p.Argv) != 0 {
			return fmt.Errorf("invalid tcp probe")
		}
	case "exec":
		if p.Port != nil || p.Path != nil || len(p.Argv) == 0 {
			return fmt.Errorf("invalid exec probe")
		}
		argvBytes := 0
		for _, item := range p.Argv {
			argvBytes += len(item)
			if item == "" || strings.ContainsRune(item, '\x00') || len(item) > 4096 {
				return fmt.Errorf("invalid exec probe")
			}
		}
		if argvBytes > 32_768 {
			return fmt.Errorf("invalid exec probe")
		}
	default:
		return fmt.Errorf("invalid probe kind")
	}
	return nil
}

func (r resources) validate() error {
	if r.CPUMillis <= 0 || r.CPUMillis > 128_000 ||
		r.MemoryMiB <= 0 || r.MemoryMiB > 1_048_576 ||
		r.EphemeralStorageMiB <= 0 || r.EphemeralStorageMiB > 1_048_576 {
		return fmt.Errorf("resource values are outside supported bounds")
	}
	return nil
}

var (
	fixtureName            = regexp.MustCompile(`^[a-z][a-z0-9-]{0,54}$`)
	fixtureHostname        = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)*$`)
	fixtureNumericHostname = regexp.MustCompile(`^(?:[0-9]+|0x[0-9a-f]+)(?:\.(?:[0-9]+|0x[0-9a-f]+))*$`)
	fixtureReserved        = map[string]bool{
		"localhost": true, "localhost.localdomain": true, "ip6-localhost": true, "ip6-loopback": true,
		"execution": true, "runtime-materializer": true, "agent": true, "verifier": true,
		"task-sandbox": true, "verifier-sandbox": true,
	}
)

func validFixtureHostname(value string) bool {
	return len(value) <= 253 && fixtureHostname.MatchString(value) && !fixtureReserved[value] &&
		!strings.HasSuffix(value, ".localhost") && !fixtureNumericHostname.MatchString(value)
}
