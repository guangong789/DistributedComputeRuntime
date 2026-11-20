package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestDiagnosticTerminalColorPolicy(t *testing.T) {
	t.Setenv("TERM", "xterm-256color")
	t.Setenv("NO_COLOR", "")
	if err := os.Unsetenv("NO_COLOR"); err != nil {
		t.Fatal(err)
	}

	file, err := os.CreateTemp(t.TempDir(), "redirect*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	defer writer.Close()
	device, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer device.Close()
	if diagnosticColor(&bytes.Buffer{}) || diagnosticColor(file) || diagnosticColor(writer) || diagnosticColor(device) {
		t.Fatal("ANSI enabled for a nonterminal destination")
	}

	// A real Linux pseudoterminal exercises the same ioctl as stdout.
	master, err := os.OpenFile("/dev/ptmx", os.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		t.Skipf("pseudoterminal unavailable: %v", err)
	}
	defer master.Close()
	if err := unix.IoctlSetPointerInt(int(master.Fd()), unix.TIOCSPTLCK, 0); err != nil {
		t.Fatal(err)
	}
	number, err := unix.IoctlGetInt(int(master.Fd()), unix.TIOCGPTN)
	if err != nil {
		t.Fatal(err)
	}
	slave, err := os.OpenFile(filepath.Join("/dev/pts", fmt.Sprint(number)), os.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer slave.Close()
	if err := unix.IoctlSetWinsize(int(slave.Fd()), unix.TIOCSWINSZ, &unix.Winsize{Col: 24, Row: 8}); err != nil {
		t.Fatal(err)
	}
	if !diagnosticColor(slave) {
		t.Fatal("ANSI disabled for a narrow Linux terminal")
	}
	for _, value := range []string{"", "1"} {
		t.Run("NO_COLOR="+value, func(t *testing.T) {
			t.Setenv("NO_COLOR", value)
			if diagnosticColor(slave) {
				t.Fatal("ANSI enabled with NO_COLOR present")
			}
		})
	}
	for _, term := range []string{"dumb", ""} {
		t.Run("TERM="+term, func(t *testing.T) {
			t.Setenv("TERM", term)
			if diagnosticColor(slave) {
				t.Fatal("ANSI enabled for an unsupported TERM value")
			}
		})
	}
}
