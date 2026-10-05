//go:build !linux

package worker

import (
	"os/exec"
	"syscall"
)

// The server targets Linux; elsewhere only the direct child is signalled.
func procAttr() *syscall.SysProcAttr { return nil }

func terminateGroup(cmd *exec.Cmd) { killGroup(cmd) }

func killGroup(cmd *exec.Cmd) {
	if cmd.Process != nil {
		_ = cmd.Process.Kill()
	}
}
