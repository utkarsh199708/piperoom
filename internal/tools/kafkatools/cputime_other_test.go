//go:build !linux && !windows

package kafkatools

import "time"

// workTime returns how long f ran (wall time; the Linux build counts the
// thread's CPU time instead, the Windows build reads the performance
// counter).
func workTime(f func()) time.Duration {
	start := time.Now()
	f()
	return time.Since(start)
}
