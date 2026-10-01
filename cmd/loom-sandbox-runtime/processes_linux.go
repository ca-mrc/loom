//go:build linux

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	signalSent = iota
	signalGone
	signalDenied
	signalFailed
)

func classifySignal(err error) (int, int) {
	if err == nil {
		return 0, signalSent
	}
	if errors.Is(err, syscall.ESRCH) {
		return int(syscall.ESRCH), signalGone
	}
	errno := int(syscall.EIO)
	var number syscall.Errno
	if errors.As(err, &number) && number > 0 && number <= 255 {
		errno = int(number)
	}
	if errors.Is(err, syscall.EPERM) {
		return errno, signalDenied
	}
	return errno, signalFailed
}

func stopProcesses(ctx context.Context) error {
	// The runtime must be PID 1 of its own container. Never run this operation
	// from a host process or a Pod that shares the trusted agent's PID namespace.
	if os.Getpid() != 1 {
		return errCleanupPIDNamespace
	}
	deadline, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	expectedUID := os.Geteuid()
	lastKill := map[int]int{}
	for {
		entries, err := os.ReadDir("/proc")
		if err != nil {
			return errCleanupProcRead
		}
		alive := false
		for _, entry := range entries {
			pid, err := strconv.Atoi(entry.Name())
			if err != nil || pid <= 1 {
				continue
			}
			directory := filepath.Join("/proc", entry.Name())
			state, err := sandboxProcessState(directory, expectedUID)
			if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
				continue
			}
			if err != nil {
				var owner *processOwnerError
				if errors.As(err, &owner) {
					owner.PID = pid
				}
				return err
			}
			if state == "" {
				// An external OCI exec/probe has a parent outside this PID
				// namespace. It is not one of the runtime's task descendants.
				continue
			}
			if state == "Z" {
				// Reap orphan descendants adopted by this PID 1. Active execs are
				// complete before this explicit snapshot boundary is invoked.
				var status syscall.WaitStatus
				_, _ = syscall.Wait4(pid, &status, syscall.WNOHANG, nil)
				continue
			}
			errno, kind := classifySignal(syscall.Kill(pid, syscall.SIGKILL))
			switch kind {
			case signalGone:
				continue
			case signalDenied, signalFailed:
				cause := errCleanupSignalFailed
				if kind == signalDenied {
					cause = errCleanupSignalDenied
				}
				failure := signalCleanupFailure(directory, pid, expectedUID, errno, cause)
				if failure == nil {
					continue
				}
				return failure
			default:
				lastKill[pid] = errno
				alive = true
			}
		}
		if !alive {
			return nil
		}
		select {
		case <-deadline.Done():
			snapshots, err := remainingCleanupSnapshots(expectedUID, lastKill)
			if err != nil || len(snapshots) == 0 {
				if err != nil {
					return err
				}
				return deadline.Err()
			}
			return &cleanupDiagnosticError{cause: deadline.Err(), snapshots: snapshots}
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func signalCleanupFailure(directory string, pid, expectedUID, errno int, cause error) error {
	snapshot, err := inspectSandboxProcess(directory, expectedUID)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
		return nil
	}
	if err != nil {
		var owner *processOwnerError
		if errors.As(err, &owner) {
			owner.PID = pid
		}
		return err
	}
	if snapshot.State == "" || snapshot.State == "Z" {
		return nil
	}
	snapshot.PID = pid
	snapshot.KillErrno = errno
	snapshot.WaitChannel = boundedWaitChannel(directory)
	return &cleanupDiagnosticError{cause: cause, snapshots: []processSnapshot{snapshot}}
}

func remainingCleanupSnapshots(expectedUID int, lastKill map[int]int) ([]processSnapshot, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, errCleanupProcRead
	}
	snapshots := make([]processSnapshot, 0, 4)
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 1 {
			continue
		}
		directory := filepath.Join("/proc", entry.Name())
		snapshot, err := inspectSandboxProcess(directory, expectedUID)
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
			continue
		}
		if err != nil {
			var owner *processOwnerError
			if errors.As(err, &owner) {
				owner.PID = pid
			}
			return nil, err
		}
		if snapshot.State == "" || snapshot.State == "Z" {
			continue
		}
		snapshot.PID = pid
		if errno, ok := lastKill[pid]; ok {
			snapshot.KillErrno = errno
		}
		snapshot.WaitChannel = boundedWaitChannel(directory)
		snapshots = append(snapshots, snapshot)
	}
	sort.SliceStable(snapshots, func(i, j int) bool {
		if snapshots[i].SignalPending != snapshots[j].SignalPending {
			return snapshots[i].SignalPending
		}
		if (snapshots[i].State == "D") != (snapshots[j].State == "D") {
			return snapshots[i].State == "D"
		}
		return snapshots[i].PID < snapshots[j].PID
	})
	if len(snapshots) > 4 {
		snapshots = snapshots[:4]
	}
	return snapshots, nil
}

func boundedWaitChannel(directory string) string {
	payload, err := os.ReadFile(filepath.Join(directory, "wchan"))
	if err != nil || len(payload) == 0 || len(payload) > 64 {
		return "unavailable"
	}
	value := strings.TrimSpace(string(payload))
	if !waitChannelPattern.MatchString(value) {
		return "unavailable"
	}
	return value
}

// Read ownership and state from one kernel proc status snapshot. PPid 0 means
// the parent lives outside this PID namespace (for example a kubelet exec
// probe, which briefly runs as root before OCI applies the container UID).
// Return an empty state for these external processes: neither kill them nor
// apply the task-child UID guard. Orphan task descendants are adopted by PID 1
// and must still pass that guard for nonroot sandboxes. Explicit root task
// containers may launch children that drop UID (package maintainer scripts,
// for example); the PID-1/private-namespace guard still confines cleanup.
// A missing/reaped proc entry is handled above.
func sandboxProcessState(directory string, expectedUID int) (string, error) {
	process, err := inspectSandboxProcess(directory, expectedUID)
	return process.State, err
}

func inspectSandboxProcess(directory string, expectedUID int) (processSnapshot, error) {
	status, err := os.ReadFile(filepath.Join(directory, "status"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
			return processSnapshot{}, err
		}
		return processSnapshot{}, errCleanupProcRead
	}
	state := ""
	var effectiveUID uint64
	var parentPID uint64
	signalPending := false
	haveUID := false
	haveParent := false
	for _, line := range strings.Split(string(status), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "PPid:":
			if len(fields) != 2 {
				return processSnapshot{}, errCleanupProcRead
			}
			parentPID, err = strconv.ParseUint(fields[1], 10, 31)
			if err != nil {
				return processSnapshot{}, errCleanupProcRead
			}
			haveParent = true
		case "State:":
			if len(fields) < 2 || len(fields[1]) != 1 {
				return processSnapshot{}, errCleanupProcRead
			}
			state = fields[1]
		case "Uid:":
			// Linux reports real, effective, saved-set and filesystem UIDs.
			if len(fields) != 5 {
				return processSnapshot{}, errCleanupProcRead
			}
			effectiveUID, err = strconv.ParseUint(fields[2], 10, 32)
			if err != nil {
				return processSnapshot{}, errCleanupProcRead
			}
			haveUID = true
		case "SigPnd:", "ShdPnd:":
			if len(fields) != 2 {
				return processSnapshot{}, errCleanupProcRead
			}
			mask, err := strconv.ParseUint(fields[1], 16, 64)
			if err != nil {
				return processSnapshot{}, errCleanupProcRead
			}
			if mask&(1<<(syscall.SIGKILL-1)) != 0 {
				signalPending = true
			}
		}
	}
	if state == "" || !haveUID || !haveParent {
		return processSnapshot{}, errCleanupProcRead
	}
	if parentPID == 0 {
		return processSnapshot{}, nil
	}
	if expectedUID != 0 && effectiveUID != uint64(expectedUID) {
		return processSnapshot{}, &processOwnerError{State: state, ParentPID: parentPID, ExpectedUID: expectedUID, ObservedUID: effectiveUID}
	}
	return processSnapshot{
		ParentPID: parentPID, State: state, UID: effectiveUID, SignalPending: signalPending,
	}, nil
}
