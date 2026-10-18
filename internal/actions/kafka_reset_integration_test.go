//go:build integration

package actions

import (
	"net"
	"strings"
	"testing"
	"time"

	"piperoom/internal/config"
)

// TestResetConsumerOffsetsAgainstDevStack runs runResetEndToEnd (it_ topic,
// pipeline and group, cleaned up) against the dev stack's Kafka.
func TestResetConsumerOffsetsAgainstDevStack(t *testing.T) {
	brokers := strings.Split(config.Load().KafkaBrokers, ",")
	conn, err := net.DialTimeout("tcp", brokers[0], 2*time.Second)
	if err != nil {
		t.Skipf("kafka not reachable at %s: %v", brokers[0], err)
	}
	_ = conn.Close()
	runResetEndToEnd(t, brokers)
}
