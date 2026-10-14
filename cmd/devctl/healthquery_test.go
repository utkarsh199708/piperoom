package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestQueryTextTargetDown: health reads the DB through the configured URL; a
// dead URL is an error naming no password, and it returns within the deadline.
func TestQueryTextTargetDown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := queryText(ctx, "postgres://u:s3cret-pw@127.0.0.1:1/orders?sslmode=disable&connect_timeout=2", "SELECT '1'")
	if err == nil {
		t.Fatal("want an error for a closed port")
	}
	if strings.Contains(err.Error(), "s3cret-pw") {
		t.Fatalf("error leaks the password: %v", err)
	}
}

func TestQueryTextBadURL(t *testing.T) {
	if _, err := queryText(context.Background(), "::not a url", "SELECT '1'"); err == nil {
		t.Fatal("want an error for a bad URL")
	}
}
