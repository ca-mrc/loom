package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func materialize(arguments []string) error {
	flags := flag.NewFlagSet("materialize", flag.ContinueOnError)
	encodedPlan := flags.String("encoded-plan", "", "base64-encoded immutable plan")
	runtimeDestination := flags.String("runtime-dest", "/loom/runtime/loom-execution-runtime", "runtime binary destination")
	planDestination := flags.String("plan-dest", "/loom/runtime/execution-plan.json", "plan destination")
	sandboxSource := flags.String("sandbox-source", "/loom-sandbox-runtime", "bundled sandbox binary")
	sandboxDestination := flags.String("sandbox-dest", "/loom/runtime/loom-sandbox-runtime", "sandbox binary destination")
	guestSource := flags.String("guest-source", "/guest", "bundled guest payload")
	guestDestination := flags.String("guest-dest", "/loom/runtime/guest", "guest payload destination")
	sandboxRoot := flags.String("sandbox-root", "/loom/sandboxes", "private sandbox volume root")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if len(*encodedPlan) == 0 || len(*encodedPlan) > 512*1024 {
		return fmt.Errorf("encoded plan size is invalid")
	}
	payload, err := base64.RawURLEncoding.DecodeString(*encodedPlan)
	if err != nil {
		return fmt.Errorf("decode encoded plan: %w", err)
	}
	p, err := decodePlan(payload)
	if err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	binary, err := os.ReadFile(executable)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(binary)
	actual := "sha256:" + hex.EncodeToString(digest[:])
	if actual != p.RuntimeBinarySHA256 {
		return fmt.Errorf("runtime binary digest mismatch")
	}
	for _, destination := range []string{*runtimeDestination, *planDestination} {
		if !filepath.IsAbs(destination) || filepath.Clean(destination) != destination {
			return fmt.Errorf("materialization destination must be a clean absolute path")
		}
		if err := secureDirectory(filepath.Dir(destination)); err != nil {
			return err
		}
	}
	if err := writeExclusive(*runtimeDestination, binary, 0o555); err != nil {
		return err
	}
	if err := writeExclusive(*planDestination, payload, 0o444); err != nil {
		_ = os.Remove(*runtimeDestination)
		return err
	}
	completed := false
	defer func() {
		if !completed {
			_ = os.Remove(*runtimeDestination)
			_ = os.Remove(*planDestination)
		}
	}()
	for _, sidecar := range p.Sidecars {
		if !sidecar.PrivateSandbox {
			continue
		}
		if !filepath.IsAbs(*sandboxDestination) || filepath.Clean(*sandboxDestination) != *sandboxDestination {
			return fmt.Errorf("sandbox destination must be a clean absolute path")
		}
		if err := secureDirectory(filepath.Dir(*sandboxDestination)); err != nil {
			return err
		}
		// Both binaries arrive in the same already-admitted OCI image.
		sandbox, err := os.ReadFile(*sandboxSource)
		if err != nil {
			return err
		}
		if err := writeExclusive(*sandboxDestination, sandbox, 0o555); err != nil {
			return err
		}
		break
	}
	for _, sidecar := range p.Sidecars {
		if sidecar.GuestExecution == nil {
			continue
		}
		// Leave 128 MiB for ordinary binaries, the immutable plan and metadata.
		// The validated guest class already requires a >=1024 MiB runtime volume.
		if err := materializeGuestPayload(*guestSource, *guestDestination, (p.RuntimeVolumeMiB-128)*1024*1024); err != nil {
			return err
		}
		break
	}
	if err := materializeNetworkFiles(p.Sidecars, *sandboxRoot, "/etc"); err != nil {
		return err
	}
	completed = true
	return nil
}

// Container runtimes may mount the same Pod hosts/resolver files into multiple
// containers. Copy them into each sandbox's own emptyDir before starting any
// untrusted process; the renderer binds only that role's files into /etc.
func materializeNetworkFiles(sidecars []sidecar, sandboxRoot, sourceRoot string) error {
	aliases := []string{}
	for _, item := range sidecars {
		if !item.TaskFixture {
			continue
		}
		if item.Hostname == nil || !validFixtureHostname(*item.Hostname) || len(aliases) != 0 {
			return fmt.Errorf("only one valid fixture hostname is supported")
		}
		aliases = append(aliases, *item.Hostname)
	}
	for _, sidecar := range sidecars {
		if !sidecar.PrivateSandbox {
			continue
		}
		if !filepath.IsAbs(sandboxRoot) || filepath.Clean(sandboxRoot) != sandboxRoot {
			return fmt.Errorf("sandbox root must be a clean absolute path")
		}
		if err := secureDirectory(sandboxRoot); err != nil {
			return err
		}
		root := filepath.Join(sandboxRoot, sidecar.RoleName)
		if err := secureDirectory(root); err != nil {
			return err
		}
		network := filepath.Join(root, "network")
		// Never follow or reuse an existing directory or file. Initialization
		// runs once, before the private volume is visible to task processes.
		if err := os.Mkdir(network, 0o755); err != nil {
			return err
		}
		for _, name := range []string{"hosts", "resolv.conf"} {
			body, err := os.ReadFile(filepath.Join(sourceRoot, name))
			if err != nil {
				return err
			}
			if name == "hosts" && len(aliases) != 0 {
				for _, line := range strings.Split(string(body), "\n") {
					fields := strings.Fields(strings.SplitN(line, "#", 2)[0])
					for index, field := range fields {
						if index > 0 && strings.EqualFold(field, aliases[0]) {
							return fmt.Errorf("fixture hostname conflicts with existing hosts entry")
						}
					}
				}
				if len(body) > 0 && body[len(body)-1] != '\n' {
					body = append(body, '\n')
				}
				body = append(body, []byte("127.0.0.1 "+aliases[0]+"\n")...)
			}
			if err := writeExclusive(filepath.Join(network, name), body, 0o644); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeExclusive(path string, payload []byte, mode os.FileMode) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return err
	}
	return file.Close()
}
