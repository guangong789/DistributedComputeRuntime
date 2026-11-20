package externalcompute

import (
	"os/exec"
	"runtime"
	"syscall"
)

func runCommand(cmd *exec.Cmd) error {
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}

	// Linux ties Pdeathsig to the creating thread. Reserve that thread
	// through the sole Wait so another goroutine cannot retire it.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	return cmd.Run()
}
