//go:build !linux

package main

import "os/exec"

func applyTaskCredential(*exec.Cmd, *guestTaskIdentity) {}

func withTaskFS(_ *guestTaskIdentity, fn func() error) error { return fn() }
