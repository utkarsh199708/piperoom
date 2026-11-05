//go:build integration

package actions

import (
	"net"
	"strings"
	"testing"
	"time"

	"piperoom/internal/config"
)

// TestDLQReplayAgainstDevStack runs runDLQReplayEndToEnd (it_ topics and
// pipeline, cleaned up) against the dev stack's Kafka: the target's end
// offset grows by the replayed count, the DLQ's does not move.
func TestDLQReplayAgainstDevStack(t *testing.T) {
	brokers := strings.Split(config.Load().KafkaBrokers, ",")
	conn, err := net.DialTimeout("tcp", brokers[0], 2*time.Second)
	if err != nil {
		t.Skipf("kafka not reachable at %s: %v", brokers[0], err)
	}
	_ = conn.Close()
	runDLQReplayEndToEnd(t, brokers)
}
