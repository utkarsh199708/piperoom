//go:build linux

package kafkatools

import (
	"runtime"
	"syscall"
	"time"
)

// workTime returns how long f ran on the calling thread's CPU: unlike wall
// time it leaves out time the thread waits while other tests' packages hold
// the CPUs (CI runs them in parallel), so a budget is about the work alone.
func workTime(f func()) time.Duration {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	var before, after syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_THREAD, &before) != nil {
		start := time.Now()
		f()
		return time.Since(start)
	}
	f()
	if syscall.Getrusage(syscall.RUSAGE_THREAD, &after) != nil {
		return 0
	}
	cpu := func(r syscall.Rusage) time.Duration {
		return time.Duration(r.Utime.Nano() + r.Stime.Nano())
	}
	return cpu(after) - cpu(before)
}
