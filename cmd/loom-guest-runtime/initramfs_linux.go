//go:build linux

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// guestBootConfig contains only task-visible settings, never host paths,
// provider credentials, controller tokens or verifier inputs.
type guestBootConfig struct {
	Environment  []string `json:"environment"`
	MaxTransfer  int64    `json:"max_transfer_bytes"`
	ExecTimeout  int      `json:"exec_timeout_seconds"`
	NestedDocker bool     `json:"nested_docker"`
}

func (c guestConfig) writeInitramfs() error {
	out, err := os.OpenFile(filepath.Join(c.State, "initrd"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer out.Close()
	var inode uint32
	entry := func(name string, mode uint32, data []byte) error {
		inode++
		return writeCPIO(out, inode, name, mode, data)
	}
	for _, name := range []string{"proc", "sys", "dev", "lower", "state", "root", "payload", "modules"} {
		if err := entry(name, 0040755, nil); err != nil {
			return err
		}
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	binary, err := os.ReadFile(executable)
	if err != nil {
		return err
	}
	if err := entry("init", 0100755, binary); err != nil {
		return err
	}
	config, err := json.Marshal(guestBootConfig{os.Environ(), c.MaxTransfer, c.ExecTimeout, c.NestedDocker})
	if err != nil {
		return err
	}
	if err := entry("config.json", 0100600, config); err != nil {
		return err
	}
	for _, name := range []string{"netfs", "9pnet", "9pnet_virtio", "9p", "overlay"} {
		data, err := os.ReadFile(filepath.Join(c.Payload, "boot-modules", name+".ko"))
		if err != nil {
			return err
		}
		if err := entry("modules/"+name+".ko", 0100600, data); err != nil {
			return err
		}
	}
	if err := entry("TRAILER!!!", 0, nil); err != nil {
		return err
	}
	return out.Close()
}

func writeCPIO(out io.Writer, inode uint32, name string, mode uint32, data []byte) error {
	// Linux's newc format: 110-byte ASCII header, NUL-terminated name, then
	// separately aligned data. No shell, cpio executable or host mount is needed.
	header := fmt.Sprintf("070701%08x%08x%08x%08x%08x%08x%08x%08x%08x%08x%08x%08x%08x", inode, mode, 0, 0, 1, 0, len(data), 0, 0, 0, 0, len(name)+1, 0)
	for _, part := range [][]byte{[]byte(header), []byte(name + "\x00"), make([]byte, (4-(110+len(name)+1)%4)%4), data, make([]byte, (4-len(data)%4)%4)} {
		if _, err := out.Write(part); err != nil {
			return err
		}
	}
	return nil
}
