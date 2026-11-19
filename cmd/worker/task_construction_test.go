package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWorkerBuildOrdinaryTasks(t *testing.T) {
	for _, tc := range []struct {
		name, kind, payload, output, buildError, executeError string
		cancel                                                bool
	}{
		{name: "hash", kind: "hash", payload: "hello", output: "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"},
		{name: "empty hash fails at execution", kind: "hash", executeError: "hash input cannot be empty"},
		{name: "sleep", kind: "sleep", payload: "0s", output: "slept for 0s"},
		{name: "negative sleep", kind: "sleep", payload: "-1s", output: "slept for -1s"},
		{name: "cancel sleep", kind: "sleep", payload: "1h", cancel: true, executeError: context.Canceled.Error()},
		{name: "invalid sleep", kind: "sleep", payload: "invalid", buildError: `time: invalid duration "invalid"`},
		{name: "unknown", kind: "unknown", buildError: "unknown task type: unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			task, err := (&WorkerService{}).buildTask(tc.kind, tc.payload)
			if tc.buildError != "" {
				if task != nil || errorString(err) != tc.buildError {
					t.Fatalf("task=%#v error=%v, want %q", task, err, tc.buildError)
				}
				return
			}
			if err != nil || task == nil {
				t.Fatalf("build task: task=%#v error=%v", task, err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if tc.cancel {
				cancel()
			}
			output, err := task.Execute(ctx)
			if output != tc.output || errorString(err) != tc.executeError {
				t.Fatalf("output=%q error=%v, want output=%q error=%q", output, err, tc.output, tc.executeError)
			}
			if tc.cancel && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation identity lost: %v", err)
			}
		})
	}
}
