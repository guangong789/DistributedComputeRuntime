//go:build !linux

package externalcompute

import "os/exec"

func runCommand(cmd *exec.Cmd) error {
	return cmd.Run()
}
