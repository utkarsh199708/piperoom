//go:build windows

package kafkatools

import (
	"syscall"
	"time"
	"unsafe"
)

var (
	kernel32         = syscall.NewLazyDLL("kernel32.dll")
	procQPC          = kernel32.NewProc("QueryPerformanceCounter")
	procQPCFrequency = kernel32.NewProc("QueryPerformanceFrequency")
)

// perfCounter calls QueryPerformanceCounter or QueryPerformanceFrequency.
// Go's clock on Windows can tick in 0.5-15.6 ms steps, so a 64 KiB line
// timed with time.Now could read 0s and make the speed test's ratio
// meaningless; the performance counter resolves well under a microsecond.
func perfCounter(p *syscall.LazyProc) (int64, bool) {
	var v int64
	if r, _, _ := p.Call(uintptr(unsafe.Pointer(&v))); r == 0 {
		return 0, false
	}
	return v, true
}

// workTime returns how long f ran (wall time from the performance counter;
// the Linux build counts the thread's CPU time instead).
func workTime(f func()) time.Duration {
	freq, ok := perfCounter(procQPCFrequency)
	before, ok2 := perfCounter(procQPC)
	if !ok || !ok2 || freq <= 0 {
		start := time.Now()
		f()
		return time.Since(start)
	}
	f()
	after, ok := perfCounter(procQPC)
	if !ok {
		return 0
	}
	ticks := after - before
	// Split to avoid overflowing ticks*1e9 on a long run.
	return time.Duration(ticks/freq)*time.Second + time.Duration(ticks%freq*int64(time.Second)/freq)
}
