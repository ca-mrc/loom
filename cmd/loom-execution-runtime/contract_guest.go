package main

import (
	"fmt"
	"slices"
	"strconv"
)

// Guest authority is an explicit, immutable launch mode. A class ID alone must
// never upgrade a historical shared-kernel plan into a new execution mechanism.
func (p plan) validateGuestExecution() error {
	authClass := p.ExecutionClassID == "linux-amd64-cpu-guest-auth-v1" || p.ExecutionClassID == "linux-amd64-cpu-guest-auth-web-v1"
	guestClass := authClass || p.ExecutionClassID == "linux-amd64-cpu-guest-v1" || p.ExecutionClassID == "linux-amd64-cpu-guest-web-v1"
	guests := 0
	var capabilities []string
	for _, s := range p.Sidecars {
		g := s.GuestExecution
		if g == nil {
			continue
		}
		guests++
		if !guestClass || !s.PrivateSandbox || (s.RoleName != "task-sandbox" && s.RoleName != "verifier-sandbox") ||
			g.SchemaVersion != "loom.guest-execution.v1" || g.Runtime != "qemu-tcg-v1" || g.Capabilities == nil {
			return fmt.Errorf("invalid guest execution authority")
		}
		for i, cap := range g.Capabilities {
			switch cap {
			case "nested_docker", "singularity_mounts", "isolated_kernel_settings":
			case "emulated_pkcs11_authentication":
				if !authClass {
					return fmt.Errorf("emulated authentication requires its guest class")
				}
			default:
				return fmt.Errorf("unsupported guest capability")
			}
			if i > 0 && g.Capabilities[i-1] >= cap {
				return fmt.Errorf("guest capabilities must be sorted and unique")
			}
		}
		if capabilities != nil && !slices.Equal(capabilities, g.Capabilities) {
			return fmt.Errorf("guest capability sets differ")
		}
		capabilities = g.Capabilities
		if authClass != slices.Contains(capabilities, "emulated_pkcs11_authentication") {
			return fmt.Errorf("guest capability declaration does not match immutable class")
		}
		if s.Identity == nil || s.Identity.RunAsUser == nil || *s.Identity.RunAsUser != 0 || s.Identity.RunAsGroup == nil || *s.Identity.RunAsGroup != 0 {
			return fmt.Errorf("guest runtime requires explicit root task identity")
		}
		if s.Resources != p.TaskResources || s.ImageRef != p.TaskImageRef {
			return fmt.Errorf("guest sandbox resources and image must match task")
		}
		if s.Resources.CPUMillis < 1000 || s.Resources.MemoryMiB < 512 || s.Resources.EphemeralStorageMiB < 160 {
			return fmt.Errorf("guest resources cannot cover runtime overhead")
		}
		socket := "/loom/sandboxes/" + s.RoleName + "/sandbox.sock"
		prefix := []string{"/loom/bin/loom-sandbox-runtime", "--socket", socket, "--exec-timeout-seconds"}
		if len(s.Argv) != 5 || !slices.Equal(s.Argv[:4], prefix) {
			return fmt.Errorf("guest sandbox argv is not canonical")
		}
		timeout, err := strconv.Atoi(s.Argv[4])
		if err != nil || timeout < 1 || timeout > 86400 || strconv.Itoa(timeout) != s.Argv[4] {
			return fmt.Errorf("invalid guest command timeout")
		}
		for _, probe := range []probe{s.StartupProbe, s.ReadinessProbe} {
			if probe.Kind != "exec" || !slices.Equal(probe.Argv, []string{"/loom/bin/loom-sandbox-runtime", "--check-socket", socket}) {
				return fmt.Errorf("guest probe must observe its canonical socket")
			}
		}
	}
	if guestClass && (guests != 2 || len(p.Sidecars) != 2 || p.RuntimeVolumeMiB < 1024 || p.ControllerResources == nil || p.AgentImageRef == nil || p.ExecutionRole != "attempt" || p.Composition != "init_payload" || p.VerifierExecution != "in_attempt" || p.Verifier == nil) {
		return fmt.Errorf("guest class requires two private guests and bounded runtime payload storage")
	}
	if guestClass {
		declared := p.TaskResources
		if p.NodeResourceAllocation != nil {
			declared = p.NodeResourceAllocation.DeclaredTask
		}
		request := *p.ControllerResources
		if p.ResourceRequests != nil && p.ResourceRequests.Controller != nil {
			request = *p.ResourceRequests.Controller
		}
		if p.ControllerResources.EphemeralStorageMiB < declared.EphemeralStorageMiB+p.RuntimeVolumeMiB || request.EphemeralStorageMiB <= p.RuntimeVolumeMiB {
			return fmt.Errorf("guest payload storage must be reserved in controller allocation and request")
		}
	}
	return nil
}
