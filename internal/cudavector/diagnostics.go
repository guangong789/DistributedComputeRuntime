package cudavector

import (
	"encoding/hex"
	"errors"
	"slices"
	"strconv"
	"strings"
)

type failedExit struct {
	error
	stage              string
	backendUnavailable bool
}

func (e *failedExit) Unwrap() error { return e.error }

// FailureStage returns a log-only category for a failed child exit, or "" for
// other outcomes. The error itself still exposes the ordinary exit message.
func FailureStage(err error) string {
	var exit *failedExit
	if errors.As(err, &exit) {
		return exit.stage
	}
	return ""
}

// BackendUnavailable reports a recognized failure of the fixed CUDA backend.
// The log-only failure stage alone does not establish backend unavailability.
func BackendUnavailable(err error) bool {
	var exit *failedExit
	return errors.As(err, &exit) && exit.backendUnavailable
}

func classifyFailure(stderr string) (stage string, backendUnavailable bool) {
	const unknown = "unknown"
	records, complete := strings.CutSuffix(stderr, "\n")
	if !complete {
		return unknown, false
	}
	// The executable checks APIs in this order. After the first failure it
	// can only report cleanup checks, which do not replace the primary stage.
	type check struct{ operation, stage string }
	steps := [...]check{
		{"cudaGetDeviceCount", "device"}, {"cudaGetDeviceProperties", "device"}, {"cudaSetDevice", "device"},
		{"device", ""}, // CUDA device banner, separate from an API check.
		{"cudaMalloc", "memory_or_transfer"}, {"host_to_device", "memory_or_transfer"},
		{"kernel_launch", "launch_or_sync"}, {"cudaDeviceSynchronize", "launch_or_sync"},
		{"device_to_host", "memory_or_transfer"}, {"cudaFree", "cleanup"}, {"cudaDeviceReset", "cleanup"},
	}
	previous, firstFailure := -1, -1
	var firstDetail string
	completePrefix, freed := true, false
	for record := range strings.SplitSeq(records, "\n") {
		operation, detail, ok := strings.Cut(record, "=")
		i := slices.IndexFunc(steps[:], func(step check) bool {
			return step.operation == operation
		})
		if !ok || i <= previous {
			return unknown, false // Unrecognized, repeated or out-of-order record.
		}
		if firstFailure < 0 && i != previous+1 {
			completePrefix = false
		}
		previous = i
		freed = freed || operation == "cudaFree"
		stage := steps[i].stage
		if firstFailure >= 0 && (stage != "cleanup" || steps[firstFailure].stage == "device" ||
			steps[firstFailure].operation == "cudaMalloc" && operation == "cudaFree") {
			return unknown, false
		}
		if stage == "" {
			if !validDeviceBanner(detail) {
				return unknown, false
			}
			continue
		}
		if detail == "cudaSuccess(0)" {
			continue
		}
		if !failedCheck(detail) {
			return unknown, false
		}
		if firstFailure < 0 {
			firstFailure, firstDetail = i, detail
		}
	}
	if firstFailure < 0 {
		return unknown, false
	}
	// A diagnostic stage can be useful even for a partial trace. Health
	// evidence requires the successful API prefix and, for a kernel fault,
	// the cleanup records emitted by the trusted CUDA executable.
	primary := steps[firstFailure]
	if completePrefix {
		switch primary.operation {
		case "cudaGetDeviceCount":
			backendUnavailable = firstDetail == "cudaErrorNoDevice(100)"
		case "kernel_launch", "cudaDeviceSynchronize":
			backendUnavailable = freed && steps[previous].operation == "cudaDeviceReset" &&
				(firstDetail == "cudaErrorLaunchFailure(719)" || firstDetail == "cudaErrorIllegalAddress(700)")
		}
	}
	return primary.stage, backendUnavailable
}

func failedCheck(detail string) bool {
	detail, closed := strings.CutSuffix(detail, ")")
	name, rawCode, found := strings.Cut(detail, "(")
	suffix, isError := strings.CutPrefix(name, "cudaError")
	if !closed || !found || !isError || suffix == "" || strings.ContainsFunc(suffix, func(r rune) bool {
		return !(r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9')
	}) {
		return false
	}
	code, err := strconv.ParseInt(rawCode, 10, 32)
	return err == nil && code != 0 && strconv.FormatInt(code, 10) == rawCode
}

func validDeviceBanner(detail string) bool {
	name, tail, found := strings.Cut(detail, " cc=")
	name, named := strings.CutPrefix(name, "0 name=")
	if !found || !named || len(name) > 128 || strings.ContainsAny(name, "\x00\r") {
		return false
	}
	capability, uuid, found := strings.Cut(tail, " uuid=")
	major, minor, dotted := strings.Cut(capability, ".")
	_, majorErr := strconv.ParseUint(major, 10, 32)
	_, minorErr := strconv.ParseUint(minor, 10, 32)
	if !found || !dotted || majorErr != nil || minorErr != nil || len(uuid) != 36 ||
		uuid[8] != '-' || uuid[13] != '-' || uuid[18] != '-' || uuid[23] != '-' {
		return false
	}
	decoded, err := hex.DecodeString(strings.ReplaceAll(uuid, "-", ""))
	return err == nil && len(decoded) == 16
}
