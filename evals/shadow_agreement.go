package evals

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
)

// maxShadowFile caps one saved response; real ones with data are a few MB.
const maxShadowFile = 64 << 20

// ShadowDoc is the part of a saved GET /api/rooms/{id}/shadow?include=data,room
// response that agreement scoring reads. Everything else (events, data) is
// ignored, so the tool depends on the JSON shape only.
type ShadowDoc struct {
	Run struct {
		RoomID          string          `json:"room_id"`
		Status          string          `json:"status"`
		PipelineMap     string          `json:"pipeline_map"`
		FinalHypothesis json.RawMessage `json:"final_hypothesis"`
	} `json:"run"`
	Room *struct {
		Labelled json.RawMessage `json:"labelled"`
		Graded   json.RawMessage `json:"graded"`
	} `json:"room"`
}

// HumanGrade is the shadow.graded payload (docs/api.md): the human's verdict
// on the run. RootCause is "yes", "partial" or "no".
type HumanGrade struct {
	LayerCorrect bool   `json:"layer_correct"`
	RootCause    string `json:"root_cause"`
}

// AgreementRow is the scoring of one saved shadow run.
type AgreementRow struct {
	File       string      `json:"file"`
	RoomID     string      `json:"room_id"`
	Status     string      `json:"status"`
	LabelLayer string      `json:"label_layer"`
	AgentLayer string      `json:"agent_layer"`
	LayerAgree bool        `json:"layer_agree"`
	CauseAgree bool        `json:"cause_agree"`
	Method     string      `json:"method"`
	Reason     string      `json:"reason"`
	Human      *HumanGrade `json:"human_grade,omitempty"`
}

// Skipped is a file that could not be scored for a reason the operator can
// fix (no label in the response).
type Skipped struct {
	File   string `json:"file"`
	RoomID string `json:"room_id,omitempty"`
	Reason string `json:"reason"`
}

// Rate is n of d with the ratio; Rate is 0 when d is 0.
type Rate struct {
	N    int     `json:"n"`
	Of   int     `json:"of"`
	Rate float64 `json:"rate"`
}

func rate(n, d int) Rate {
	r := Rate{N: n, Of: d}
	if d > 0 {
		r.Rate = float64(n) / float64(d)
	}
	return r
}

// HumanAgreement compares the automatic verdict with the human's grade, for
// graded rooms only. Cause: automatic cause_agree is compared with
// root_cause "yes" (true) or "no" (false); "partial" has no boolean to
// compare with and is counted apart, outside the rate. Layer: automatic
// layer_agree is compared with the human's layer_correct. A room whose run
// has no hypothesis is still compared (both say "no").
type HumanAgreement struct {
	Graded       int  `json:"graded"`
	Partial      int  `json:"partial"`
	CauseMatch   Rate `json:"cause_match"`
	LayerMatch   Rate `json:"layer_match"`
	AutoYesHumNo int  `json:"auto_agree_human_no"`
	AutoNoHumYes int  `json:"auto_disagree_human_yes"`
}

// AgreementSummary is the totals of an agreement run.
type AgreementSummary struct {
	Rooms      int             `json:"rooms"` // scored rooms: every labelled room, whatever its status
	Skipped    []Skipped       `json:"skipped,omitempty"`
	LayerAgree Rate            `json:"layer_agree"`
	CauseAgree Rate            `json:"cause_agree"`
	BothAgree  Rate            `json:"both_agree"` // the agreement rate: layer and cause
	ByStatus   map[string]int  `json:"by_status"`
	Human      *HumanAgreement `json:"agreement_with_human_grade,omitempty"` // nil when no room has a grade
	Rows       []AgreementRow  `json:"rooms_detail"`
}

// AgreementFunc scores one hypothesis against a label.
type AgreementFunc func(ctx context.Context, hypothesis, pipelineMap string, label Label) (AgreementResult, error)

// TermsAgreement is the deterministic AgreementFunc.
func TermsAgreement(_ context.Context, hypothesis, pipelineMap string, label Label) (AgreementResult, error) {
	return Agreement(hypothesis, label, WithPipelineMap(pipelineMap))
}

// JudgeAgreement is an AgreementFunc that asks j.
func JudgeAgreement(j HypothesisJudge) AgreementFunc {
	return func(ctx context.Context, hypothesis, _ string, label Label) (AgreementResult, error) {
		return AgreementJudged(ctx, j, hypothesis, label)
	}
}

// ErrRunUnfinished is returned for a saved run that is queued or running.
var ErrRunUnfinished = errors.New("evals: shadow run is not finished")

// ScoreShadowDoc scores one parsed response. It refuses a run that is still
// queued or running: the blind rule (docs/design/shadow.md 3.5) allows
// nobody to look at an unfinished run's output, and an unfinished run has no
// final verdict to agree with anyway.
func ScoreShadowDoc(ctx context.Context, file string, doc ShadowDoc, score AgreementFunc) (*AgreementRow, *Skipped, error) {
	switch doc.Run.Status {
	case "queued", "running":
		return nil, nil, fmt.Errorf("%s: status %q: %w; refusing to score it", file, doc.Run.Status, ErrRunUnfinished)
	case "hypothesised", "no_hypothesis", "error", "budget", "interrupted":
	default:
		return nil, nil, fmt.Errorf("%s: unknown run status %q", file, doc.Run.Status)
	}
	if doc.Room == nil || isNull(doc.Room.Labelled) {
		return nil, &Skipped{File: file, RoomID: doc.Run.RoomID, Reason: "no label in the response (save GET /shadow with ?include=room after the blind label)"}, nil
	}
	var ev struct {
		Payload Label `json:"payload"`
	}
	if err := json.Unmarshal(doc.Room.Labelled, &ev); err != nil {
		return nil, nil, fmt.Errorf("%s: room.labelled: %w", file, err)
	}
	label := ev.Payload

	hyp, err := hypothesisText(doc.Run.FinalHypothesis)
	if err != nil {
		return nil, nil, fmt.Errorf("%s: run.final_hypothesis: %w", file, err)
	}
	row := &AgreementRow{File: file, RoomID: doc.Run.RoomID, Status: doc.Run.Status, LabelLayer: label.Layer}
	res, err := score(ctx, hyp, doc.Run.PipelineMap, label)
	if err != nil {
		return nil, nil, fmt.Errorf("%s: %w", file, err)
	}
	row.AgentLayer, row.LayerAgree, row.CauseAgree, row.Method, row.Reason = res.AgentLayer, res.LayerAgree, res.CauseAgree, res.Method, res.Reason
	if res.Method == AgreementJudge && row.AgentLayer == "" {
		row.AgentLayer = "-"
	}

	if !isNull(doc.Room.Graded) {
		g, err := lastGrade(doc.Room.Graded)
		if err != nil {
			return nil, nil, fmt.Errorf("%s: room.graded: %w", file, err)
		}
		row.Human = g
	}
	return row, nil, nil
}

func isNull(r json.RawMessage) bool {
	s := strings.TrimSpace(string(r))
	return s == "" || s == "null"
}

// hypothesisText reads run.final_hypothesis: an object {text, ...}, or a
// bare string; null means the run ended without one.
func hypothesisText(r json.RawMessage) (string, error) {
	if isNull(r) {
		return "", nil
	}
	var s string
	if json.Unmarshal(r, &s) == nil {
		return s, nil
	}
	var o struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(r, &o); err != nil {
		return "", err
	}
	return o.Text, nil
}

// lastGrade reads room.graded: the event (payload inside) or a list of them,
// of which the last wins as in the server.
func lastGrade(r json.RawMessage) (*HumanGrade, error) {
	var list []json.RawMessage
	if json.Unmarshal(r, &list) == nil {
		if len(list) == 0 {
			return nil, nil
		}
		r = list[len(list)-1]
	}
	var ev struct {
		Payload HumanGrade `json:"payload"`
	}
	if err := json.Unmarshal(r, &ev); err != nil {
		return nil, err
	}
	switch ev.Payload.RootCause {
	case "yes", "partial", "no":
	default:
		return nil, fmt.Errorf("root_cause %q is not yes, partial or no", ev.Payload.RootCause)
	}
	return &ev.Payload, nil
}

// ShadowFiles lists the *.json files of dir, or path itself when it is a file.
func ShadowFiles(path string) ([]string, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("evals: %w", err)
	}
	if !fi.IsDir() {
		return []string{path}, nil
	}
	ents, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("evals: %w", err)
	}
	var out []string
	for _, e := range ents {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
			out = append(out, filepath.Join(path, e.Name()))
		}
	}
	sort.Strings(out)
	if len(out) == 0 {
		return nil, fmt.Errorf("evals: no *.json files in %s", path)
	}
	return out, nil
}

// ReadShadowDoc loads one saved response from disk.
func ReadShadowDoc(file string) (ShadowDoc, error) {
	fh, err := os.Open(file)
	if err != nil {
		return ShadowDoc{}, fmt.Errorf("evals: %w", err)
	}
	defer fh.Close()
	var d ShadowDoc
	if err := json.NewDecoder(io.LimitReader(fh, maxShadowFile)).Decode(&d); err != nil {
		return ShadowDoc{}, fmt.Errorf("evals: reading %s: %w", file, err)
	}
	return d, nil
}

// ScoreShadowFiles scores every saved response under path. Any queued or
// running run, or unreadable file, fails the whole call before a summary
// exists: a partial table must not stand in for a refused run.
func ScoreShadowFiles(ctx context.Context, path string, score AgreementFunc) (AgreementSummary, error) {
	files, err := ShadowFiles(path)
	if err != nil {
		return AgreementSummary{}, err
	}
	var rows []AgreementRow
	var skipped []Skipped
	var errs []error
	for _, f := range files {
		doc, err := ReadShadowDoc(f)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		row, sk, err := ScoreShadowDoc(ctx, f, doc, score)
		switch {
		case err != nil:
			errs = append(errs, err)
		case sk != nil:
			skipped = append(skipped, *sk)
		default:
			rows = append(rows, *row)
		}
	}
	if len(errs) > 0 {
		return AgreementSummary{}, errors.Join(errs...)
	}
	return Summarise(rows, skipped), nil
}

// Summarise totals the rows.
func Summarise(rows []AgreementRow, skipped []Skipped) AgreementSummary {
	s := AgreementSummary{Rooms: len(rows), Skipped: skipped, ByStatus: map[string]int{}, Rows: rows}
	var layer, cause, both int
	h := HumanAgreement{}
	var causeCmp, causeEq, layerEq int
	for _, r := range rows {
		s.ByStatus[r.Status]++
		if r.LayerAgree {
			layer++
		}
		if r.CauseAgree {
			cause++
		}
		if r.LayerAgree && r.CauseAgree {
			both++
		}
		if r.Human == nil {
			continue
		}
		h.Graded++
		if r.Human.LayerCorrect == r.LayerAgree {
			layerEq++
		}
		switch r.Human.RootCause {
		case "partial":
			h.Partial++
		default:
			humanYes := r.Human.RootCause == "yes"
			causeCmp++
			if humanYes == r.CauseAgree {
				causeEq++
			} else if r.CauseAgree {
				h.AutoYesHumNo++
			} else {
				h.AutoNoHumYes++
			}
		}
	}
	s.LayerAgree, s.CauseAgree, s.BothAgree = rate(layer, len(rows)), rate(cause, len(rows)), rate(both, len(rows))
	if h.Graded > 0 {
		h.CauseMatch, h.LayerMatch = rate(causeEq, causeCmp), rate(layerEq, h.Graded)
		s.Human = &h
	}
	return s
}

// WriteTable prints the per-room table and the totals.
func (s AgreementSummary) WriteTable(w io.Writer) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "room\tstatus\tlabel layer\tagent layer\tlayer\tcause\thuman (layer/cause)\tmethod")
	for _, r := range s.Rows {
		agent := r.AgentLayer
		if agent == "" {
			agent = "-"
		}
		human := "-"
		if r.Human != nil {
			human = fmt.Sprintf("%s/%s", yn(r.Human.LayerCorrect), r.Human.RootCause)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", r.RoomID, r.Status, r.LabelLayer, agent, yn(r.LayerAgree), yn(r.CauseAgree), human, r.Method)
	}
	if err := tw.Flush(); err != nil {
		return fmt.Errorf("evals: writing table: %w", err)
	}
	fmt.Fprintf(w, "\nrooms scored: %d", s.Rooms)
	if n := len(s.Skipped); n > 0 {
		fmt.Fprintf(w, " (%d skipped)", n)
	}
	fmt.Fprintf(w, "\nlayer agrees: %s\ncause agrees: %s\nboth agree:   %s\n", pct(s.LayerAgree), pct(s.CauseAgree), pct(s.BothAgree))
	if h := s.Human; h != nil {
		fmt.Fprintf(w, "with human grade (%d graded rooms):\n  cause: auto equals human %s (%d partial grades left out; auto yes/human no %d, auto no/human yes %d)\n  layer: auto equals human %s\n",
			h.Graded, pct(h.CauseMatch), h.Partial, h.AutoYesHumNo, h.AutoNoHumYes, pct(h.LayerMatch))
	} else {
		fmt.Fprintln(w, "with human grade: no graded rooms")
	}
	for _, k := range s.Skipped {
		fmt.Fprintf(w, "skipped %s: %s\n", k.File, k.Reason)
	}
	return nil
}

func yn(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

func pct(r Rate) string {
	if r.Of == 0 {
		return "n/a (0 rooms)"
	}
	return fmt.Sprintf("%d of %d (%.0f%%)", r.N, r.Of, 100*r.Rate)
}

// WriteJSON writes the summary as indented JSON.
func (s AgreementSummary) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(s); err != nil {
		return fmt.Errorf("evals: writing agreement summary: %w", err)
	}
	return nil
}
