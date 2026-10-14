package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"piperoom/evals"
)

// runAgreement scores saved shadow responses against their human labels. It
// only reads files: it never calls the server, so it cannot show a run to
// anyone before they have graded it, and it refuses runs that are not
// finished (see evals.ScoreShadowDoc).
func runAgreement(f flags, stdout io.Writer) (int, error) {
	score := evals.AgreementFunc(evals.TermsAgreement)
	switch f.judge {
	case "", evals.JudgeTerms:
	case evals.JudgeClaude:
		j, err := newJudge(f.judgeModel)
		if err != nil {
			return 1, err
		}
		score = evals.JudgeAgreement(j)
	default:
		return 1, fmt.Errorf("unknown -judge %q (want terms or claude)", f.judge)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	sum, err := evals.ScoreShadowFiles(ctx, f.agreement, score)
	if err != nil {
		return 1, err
	}
	if err := sum.WriteTable(stdout); err != nil {
		return 1, err
	}
	if f.out != "" {
		if err := writeFile(f.out, sum.WriteJSON); err != nil {
			return 1, err
		}
	}
	return 0, nil
}
