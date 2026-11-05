//go:build linux

package kafkatools

import (
	"runtime"
	"syscall"
	"time"
	"unsafe"
)

// clockThreadCPUTime is CLOCK_THREAD_CPUTIME_ID from <time.h>; package
// syscall does not name it.
const clockThreadCPUTime = 3

// threadCPU reads the calling thread's CPU clock. getrusage(RUSAGE_THREAD)
// counts in scheduler ticks (4 ms on some kernels), too coarse to time a
// 16 KiB line; this clock counts in nanoseconds.
func threadCPU() (time.Duration, bool) {
	var ts syscall.Timespec
	if _, _, errno := syscall.Syscall(syscall.SYS_CLOCK_GETTIME, clockThreadCPUTime, uintptr(unsafe.Pointer(&ts)), 0); errno != 0 {
		return 0, false
	}
	return time.Duration(ts.Nano()), true
}

// workTime returns how long f ran on the calling thread's CPU: unlike wall
// time it leaves out time the thread waits while other tests' packages hold
// the CPUs (CI runs them in parallel), so a budget is about the work alone.
func workTime(f func()) time.Duration {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	before, ok := threadCPU()
	if !ok {
		start := time.Now()
		f()
		return time.Since(start)
	}
	f()
	after, ok := threadCPU()
	if !ok {
		return 0
	}
	return after - before
}
