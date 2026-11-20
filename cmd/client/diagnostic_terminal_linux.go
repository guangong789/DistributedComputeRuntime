package main

import (
	"os"

	"golang.org/x/sys/unix"
)

func diagnosticTerminal(file *os.File) bool {
	// TCGETS distinguishes a terminal from other character devices, such as
	// /dev/null, and from regular files and pipes.
	_, err := unix.IoctlGetTermios(int(file.Fd()), unix.TCGETS)
	return err == nil
}
