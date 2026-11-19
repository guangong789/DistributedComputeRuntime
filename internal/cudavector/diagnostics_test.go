package cudavector

import "testing"

func TestClassifyFailure(t *testing.T) {
	for _, tc := range []struct {
		name, stderr, want string
	}{
		{"device count", "cudaGetDeviceCount=cudaErrorNoDevice(100)\n", "device"},
		{"device properties", "cudaGetDeviceProperties=cudaErrorInvalidDevice(101)\n", "device"},
		{"device selection", "cudaSetDevice=cudaErrorInvalidDevice(101)\n", "device"},
		{"allocate", "cudaMalloc=cudaErrorMemoryAllocation(2)\n", "memory_or_transfer"},
		{"copy to device", "host_to_device=cudaErrorInvalidValue(1)\n", "memory_or_transfer"},
		{"copy to host", "device_to_host=cudaErrorInvalidValue(1)\n", "memory_or_transfer"},
		{"launch", "cudaMalloc=cudaSuccess(0)\nkernel_launch=cudaErrorInvalidConfiguration(9)\n", "launch_or_sync"},
		{"synchronize", "cudaDeviceSynchronize=cudaErrorLaunchFailure(719)\n", "launch_or_sync"},
		{"free", "cudaFree=cudaErrorInvalidValue(1)\n", "cleanup"},
		{"reset", "cudaDeviceReset=cudaErrorUnknown(999)\n", "cleanup"},
		{"primary before cleanup", "kernel_launch=cudaErrorInvalidConfiguration(9)\ncudaFree=cudaErrorInvalidValue(1)\ncudaDeviceReset=cudaErrorUnknown(999)\n", "launch_or_sync"},
		{"allocation before reset", "cudaMalloc=cudaErrorMemoryAllocation(2)\ncudaDeviceReset=cudaErrorUnknown(999)\n", "memory_or_transfer"},
		{"missing", "", "unknown"},
		{"successful checks only", "cudaMalloc=cudaSuccess(0)\ncudaFree=cudaSuccess(0)\n", "unknown"},
		{"truncated", "kernel_launch=cudaErrorInvalidConfiguration(9)", "unknown"},
		{"zero failure code", "cudaMalloc=cudaErrorMemoryAllocation(0)\n", "unknown"},
		{"nonzero success code", "cudaMalloc=cudaSuccess(2)\n", "unknown"},
		{"bad token", "cudaMalloc=cudaError bad(2)\n", "unknown"},
		{"unrecognized", "new_operation=cudaErrorUnknown(999)\n", "unknown"},
		{"ambiguous failures", "host_to_device=cudaErrorInvalidValue(1)\nkernel_launch=cudaErrorInvalidConfiguration(9)\n", "unknown"},
		{"out of order", "cudaFree=cudaErrorInvalidValue(1)\nkernel_launch=cudaErrorInvalidConfiguration(9)\n", "unknown"},
		{"duplicate", "cudaMalloc=cudaErrorMemoryAllocation(2)\ncudaMalloc=cudaErrorMemoryAllocation(2)\n", "unknown"},
		{"impossible cleanup after allocation failure", "cudaMalloc=cudaErrorMemoryAllocation(2)\ncudaFree=cudaErrorInvalidValue(1)\n", "unknown"},
		{"bad device banner", "device=0 name=truncated\nkernel_launch=cudaErrorInvalidConfiguration(9)\n", "unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyFailure(tc.stderr); got != tc.want {
				t.Fatalf("classification = %q, want %q", got, tc.want)
			}
		})
	}
}
