package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/guangong789/DistributedZKRuntime/internal/externalsquare"
)

func main() {
	input, err := io.ReadAll(io.LimitReader(os.Stdin, externalsquare.MaxPayloadBytes+2))
	if err != nil {
		fmt.Fprintln(os.Stderr, "external-square: could not read input")
		os.Exit(1)
	}
	payload, newline := strings.CutSuffix(string(input), "\n")
	n, err := externalsquare.ParsePayload(payload)
	if !newline || err != nil {
		fmt.Fprintln(os.Stderr, "external-square: expected one canonical integer line")
		os.Exit(1)
	}
	if _, err := fmt.Fprintln(os.Stdout, n*n); err != nil {
		os.Exit(1)
	}
}
