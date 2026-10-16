//go:build !linux

package kafkatools

import "time"

// workTime returns how long f ran (wall time; the Linux build counts the
// thread's CPU time instead).
func workTime(f func()) time.Duration {
	start := time.Now()
	f()
	return time.Since(start)
}
