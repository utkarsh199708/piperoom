package ops

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"piperoom/internal/setup/protocol"
)

// tierOp is a minimal op with a fixed floor.
type tierOp struct {
	name, tier string
}

func (o tierOp) Name() string                                                { return o.name }
func (tierOp) Target() string                                                { return "" }
func (o tierOp) Tier() string                                                { return o.tier }
func (tierOp) Layer() string                                                 { return "environment" }
func (tierOp) Describe() string                                              { return "a test op" }
func (tierOp) ParamsSchema() map[string]any                                  { return map[string]any{"type": "object"} }
func (tierOp) Validate(json.RawMessage, Env) error                           { return nil }
func (tierOp) Execute(context.Context, json.RawMessage, Env) (Result, error) { return Result{}, nil }

// paramTierOp is tierOp with a TierFor answer.
type paramTierOp struct {
	tierOp
	tierFor func(json.RawMessage) (string, error)
}

func (o paramTierOp) TierFor(params json.RawMessage) (string, error) { return o.tierFor(params) }

var _ ParamTierer = paramTierOp{}

func TestFloorFor(t *testing.T) {
	reg, err := New(Config{ProjectDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer reg.Close()
	byFile := func(raw json.RawMessage) (string, error) {
		var p struct {
			File string `json:"file"`
		}
		if err := json.Unmarshal(raw, &p); err != nil {
			return "", err
		}
		switch p.File {
		case "root":
			return protocol.TierT1, nil
		case "outside":
			return protocol.TierT2, nil
		case "low":
			return protocol.TierT0, nil
		case "bogus":
			return "T9", nil
		}
		return "", errors.New("unclassified")
	}
	for _, op := range []Op{
		tierOp{name: "fixed_t1", tier: protocol.TierT1},
		paramTierOp{tierOp: tierOp{name: "routed_t1", tier: protocol.TierT1}, tierFor: byFile},
	} {
		if err := reg.Register(op); err != nil {
			t.Fatal(err)
		}
	}

	cases := []struct {
		op, params string
		want       string
		ok, err    bool
	}{
		// An op without TierFor answers Floor, whatever its params say.
		{"fixed_t1", `{"file":"outside"}`, protocol.TierT1, true, false},
		{"fixed_t1", `not json`, protocol.TierT1, true, false},
		// TierFor raises the floor.
		{"routed_t1", `{"file":"root"}`, protocol.TierT1, true, false},
		{"routed_t1", `{"file":"outside"}`, protocol.TierT2, true, false},
		// It never lowers it.
		{"routed_t1", `{"file":"low"}`, protocol.TierT1, true, false},
		// Unclassified params and a non-tier answer are errors.
		{"routed_t1", `{"file":"x"}`, protocol.TierT1, true, true},
		{"routed_t1", `{"file":"bogus"}`, protocol.TierT1, true, true},
		// An unknown op is not ok.
		{"no_such_op", `{}`, "", false, false},
	}
	for _, c := range cases {
		got, ok, err := reg.FloorFor(c.op, json.RawMessage(c.params))
		if got != c.want || ok != c.ok || (err != nil) != c.err {
			t.Errorf("FloorFor(%s, %s) = %q, %v, %v; want %q, %v, err=%v", c.op, c.params, got, ok, err, c.want, c.ok, c.err)
		}
		if err != nil && !errors.Is(err, ErrParams) {
			t.Errorf("FloorFor(%s, %s) error %v does not wrap ErrParams", c.op, c.params, err)
		}
		if floor, fok := reg.Floor(c.op); fok != c.ok || (c.op == "fixed_t1" && floor != got) {
			t.Errorf("Floor(%s) = %q, %v; FloorFor answered %q", c.op, floor, fok, got)
		}
	}
}
