//go:build !linux

package main

import "os"

// Use plain text where the Linux terminal check is unavailable.
func diagnosticTerminal(_ *os.File) bool { return false }
