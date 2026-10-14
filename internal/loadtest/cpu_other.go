//go:build !unix

package loadtest

// processCPUSeconds is not measured on this platform.
func processCPUSeconds() float64 { return -1 }
