//go:build linux

package main

import (
	"os/exec"
	"runtime"
	"sync"
	"syscall"
)

func applyTaskCredential(cmd *exec.Cmd, id *guestTaskIdentity) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	groups := id.groups
	if groups == nil {
		groups = []uint32{}
	}
	cmd.SysProcAttr.Credential = &syscall.Credential{Uid: id.uid, Gid: id.gid, Groups: groups}
}

var fsIdentity sync.Mutex

func withTaskFS(id *guestTaskIdentity, fn func() error) error {
	if id == nil {
		return fn()
	}
	fsIdentity.Lock()
	defer fsIdentity.Unlock()
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	previous, err := syscall.Getgroups()
	if err != nil {
		return err
	}
	groups := make([]int, len(id.groups))
	for i, group := range id.groups {
		groups[i] = int(group)
	}
	if err := syscall.Setgroups(groups); err != nil {
		return err
	}
	defer syscall.Setgroups(previous)
	previousGID := setfsgid(int(id.gid))
	defer setfsgid(previousGID)
	previousUID := setfsuid(int(id.uid))
	defer setfsuid(previousUID)
	return fn()
}

func setfsuid(uid int) int {
	previous, _, _ := syscall.Syscall(syscall.SYS_SETFSUID, uintptr(uid), 0, 0)
	return int(previous)
}

func setfsgid(gid int) int {
	previous, _, _ := syscall.Syscall(syscall.SYS_SETFSGID, uintptr(gid), 0, 0)
	return int(previous)
}
