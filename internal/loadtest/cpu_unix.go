//go:build unix

package loadtest

import "syscall"

// processCPUSeconds is this process's user + system CPU time.
func processCPUSeconds() float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return -1
	}
	return float64(ru.Utime.Nano()+ru.Stime.Nano()) / 1e9
}
