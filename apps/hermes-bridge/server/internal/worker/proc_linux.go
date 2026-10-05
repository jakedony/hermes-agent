package worker

import (
	"os/exec"
	"syscall"
)

// procAttr puts the worker in its own process group (so Ctrl-C on the server's terminal reaches
// only the server, which then stops workers in order) and asks the kernel to SIGKILL it if the
// server dies without cleaning up.
func procAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
}

func signalGroup(cmd *exec.Cmd, sig syscall.Signal) {
	if cmd.Process != nil {
		_ = syscall.Kill(-cmd.Process.Pid, sig)
	}
}

func terminateGroup(cmd *exec.Cmd) { signalGroup(cmd, syscall.SIGTERM) }
func killGroup(cmd *exec.Cmd)      { signalGroup(cmd, syscall.SIGKILL) }
