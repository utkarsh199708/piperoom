package tools

import (
	"context"
	"errors"
	"testing"
)

func TestWithPipelineIDEmptyClearsThePipeline(t *testing.T) {
	o := scopeTestOwners()
	ctx := WithPipelineID(context.Background(), "orders")
	if _, err := ScopeFor(WithPipelineID(ctx, ""), o); !errors.Is(err, ErrNoScope) {
		t.Fatalf("cleared ctx: err = %v, want ErrNoScope", err)
	}
	// A full pipeline is cleared too.
	p, _ := o.Pipeline("orders")
	if _, err := ScopeFor(WithPipelineID(WithPipeline(context.Background(), p), ""), o); !errors.Is(err, ErrNoScope) {
		t.Fatalf("cleared full pipeline: err = %v, want ErrNoScope", err)
	}
	// The explicit opt-out survives the clear.
	sc, err := ScopeFor(WithPipelineID(WithUnscoped(ctx), ""), o)
	if err != nil || !sc.Unscoped() {
		t.Fatalf("opt-out after clear: %+v, %v", sc, err)
	}
}
