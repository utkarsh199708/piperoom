package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/tools"
)

// reportingKafka is the real client's shape: it implements
// clients.KeyScanReader, whose FindByKeyScan says which partitions max_scan
// cut short.
type reportingKafka struct {
	*fakeKafka
	scan clients.KeyScan
	err  error
}

func (r *reportingKafka) FindByKeyScan(ctx context.Context, t, k string, s time.Time, m int) (clients.KeyScan, error) {
	return r.scan, r.err
}

var _ clients.KeyScanReader = (*reportingKafka)(nil)

// TestFindByKeyReportsTruncation: "not found" after a max_scan cut used to
// read as "the key is absent"; the summary and Data must say the scan
// stopped early and where.
func TestFindByKeyReportsTruncation(t *testing.T) {
	for _, tt := range []struct {
		name string
		scan clients.KeyScan
		want string
	}{
		{"not found, cut", clients.KeyScan{Truncated: []int32{0, 2}}, "not found in the scanned window"},
		{"found, cut", clients.KeyScan{
			Refs:      []clients.RecordRef{{Topic: "t", Partition: 0, Offset: 7, Timestamp: time.Now(), Key: "k"}},
			Truncated: []int32{0},
		}, "found 1 record(s)"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fk := &reportingKafka{fakeKafka: &fakeKafka{}, scan: tt.scan}
			reg := tools.NewRegistry()
			if err := Register(reg, Deps{Kafka: fk}); err != nil {
				t.Fatal(err)
			}
			tool, _ := reg.Get("kafka_find_by_key")
			r := runTool(t, tool, `{"topic":"t","key":"k","since":"2026-10-11T00:00:00Z","max_scan":10}`)
			if !strings.Contains(r.Summary, tt.want) || !strings.Contains(r.Summary, "the scan stopped (max_scan 10 or the time limit) before the end of partition(s) ") {
				t.Fatalf("summary %q: want %q and the truncation line", r.Summary, tt.want)
			}
			data, _ := r.Data.(map[string]any)
			if data["truncated"] != true || data["truncated_partitions"] == nil {
				t.Fatalf("data %+v: want truncated and truncated_partitions", data)
			}
		})
	}
}

// TestFindByKeyScanDataFields: Data carries clients.KeyScan's fields under
// its JSON names, and a complete scan states "not found" as fact.
func TestFindByKeyScanDataFields(t *testing.T) {
	at := time.Date(2026, 10, 11, 9, 0, 0, 0, time.UTC)
	for _, tt := range []struct {
		name          string
		scan          clients.KeyScan
		wantSummary   string
		wantTruncated string // JSON of truncated_partitions, "" when absent
	}{
		{"complete, none", clients.KeyScan{}, "not found: no record with key", ""},
		{"complete, found", clients.KeyScan{Refs: []clients.RecordRef{{Topic: "t", Partition: 1, Offset: 3, Timestamp: at, Key: "k"}}},
			"found 1 record(s)", ""},
		{"cut in two partitions", clients.KeyScan{
			Refs:      []clients.RecordRef{{Topic: "t", Partition: 1, Offset: 3, Timestamp: at, Key: "k"}},
			Truncated: []int32{1, 4},
		}, "before the end of partition(s) [1 4]", "[1,4]"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fk := &reportingKafka{fakeKafka: &fakeKafka{}, scan: tt.scan}
			reg := tools.NewRegistry()
			if err := Register(reg, Deps{Kafka: fk}); err != nil {
				t.Fatal(err)
			}
			tool, _ := reg.Get("kafka_find_by_key")
			r := runTool(t, tool, `{"topic":"t","key":"k","since":"2026-10-11T00:00:00Z"}`)
			if !strings.Contains(r.Summary, tt.wantSummary) {
				t.Fatalf("summary %q: want %q", r.Summary, tt.wantSummary)
			}
			raw, err := json.Marshal(r.Data)
			if err != nil {
				t.Fatal(err)
			}
			var got struct {
				Records   []clients.RecordRef `json:"records"`
				Truncated json.RawMessage     `json:"truncated_partitions"`
				Unknown   *bool               `json:"truncation_unknown"`
			}
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			if len(got.Records) != len(tt.scan.Refs) {
				t.Fatalf("records = %+v, want %+v", got.Records, tt.scan.Refs)
			}
			if string(got.Truncated) != tt.wantTruncated {
				t.Fatalf("truncated_partitions = %s, want %q", got.Truncated, tt.wantTruncated)
			}
			if got.Unknown != nil {
				t.Fatalf("truncation_unknown set for a client that reports truncation: %s", raw)
			}
		})
	}
}

// TestFindByKeyWithoutScanReader: a client without clients.KeyScanReader
// (fakes, eval replay) cannot say whether the scan was cut, so the tool never
// states "not found" or a complete list as fact (clients.KeyScanReader).
func TestFindByKeyWithoutScanReader(t *testing.T) {
	var refs []clients.RecordRef
	fk := &fakeKafka{findFn: func(ctx context.Context, topic, key string, since time.Time, max int) ([]clients.RecordRef, error) {
		return refs, nil
	}}
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Kafka: fk}); err != nil {
		t.Fatal(err)
	}
	tool, _ := reg.Get("kafka_find_by_key")
	for _, tt := range []struct {
		name  string
		refs  []clients.RecordRef
		want  string
		hedge string
	}{
		// With no match there is nothing for a match to be newer than.
		{"none", nil, "no record with key \"k\" found since 2026-10-11T00:00:00Z in the scanned window",
			"this client cannot report whether the scan was cut (max_scan 5000 or the time limit): matches may exist past the scanned window"},
		{"one", []clients.RecordRef{{Topic: "t", Partition: 0, Offset: 7, Timestamp: time.Now(), Key: "k"}}, "found 1 record(s)",
			"this client cannot report whether the scan was cut (max_scan 5000 or the time limit): newer matches may exist past the scanned window"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			refs = tt.refs
			r := runTool(t, tool, `{"topic":"t","key":"k","since":"2026-10-11T00:00:00Z"}`)
			if !strings.Contains(r.Summary, tt.want) || !strings.Contains(r.Summary, tt.hedge) {
				t.Fatalf("summary %q: want %q and %q", r.Summary, tt.want, tt.hedge)
			}
			if strings.Contains(r.Summary, "not found:") {
				t.Fatalf("summary %q states absence as fact", r.Summary)
			}
			data, _ := r.Data.(map[string]any)
			if data["truncation_unknown"] != true {
				t.Fatalf("data %+v: want truncation_unknown", data)
			}
			// "truncated": false would claim a complete scan.
			if _, ok := data["truncated"]; ok {
				t.Fatalf("data %+v: truncated must be absent when it is not known", data)
			}
		})
	}
}

// TestFindByKeyManyMatchesKeepsNote (#424): with ~170+ matches the list used
// to fill MaxSummaryBytes, so the cut dropped the truncation note and the
// newest offsets. The note must come before the list, the list must show the
// newest findMaxListed matches and count the rest, and Data keeps them all.
func TestFindByKeyManyMatchesKeepsNote(t *testing.T) {
	at := time.Date(2026, 10, 15, 16, 0, 0, 0, time.UTC)
	for _, n := range []int{100, 101, 169, 170, 171, 300} {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			refs := make([]clients.RecordRef, n)
			for i := range refs {
				refs[i] = clients.RecordRef{Topic: "t", Partition: 0, Offset: int64(i), Timestamp: at.Add(time.Duration(i) * time.Second), Key: "k"}
			}
			fk := &reportingKafka{fakeKafka: &fakeKafka{}, scan: clients.KeyScan{Refs: refs, Truncated: []int32{0}}}
			reg := tools.NewRegistry()
			if err := Register(reg, Deps{Kafka: fk}); err != nil {
				t.Fatal(err)
			}
			tool, _ := reg.Get("kafka_find_by_key")
			r := runTool(t, tool, `{"topic":"t","key":"k","since":"2026-10-11T00:00:00Z"}`)
			if r.Truncated {
				t.Errorf("summary cut at %d bytes; the list must fit", len(r.Summary))
			}
			for _, want := range []string{fmt.Sprintf("found %d record(s)", n), "the scan stopped", fmt.Sprintf("offset %d,", n-1)} {
				if !strings.Contains(r.Summary, want) {
					t.Errorf("summary lacks %q:\n%s", want, r.Summary)
				}
			}
			if listed := strings.Count(r.Summary, "\n  partition "); listed != min(n, findMaxListed) {
				t.Errorf("listed %d matches, want %d", listed, min(n, findMaxListed))
			}
			older := fmt.Sprintf("\n%d older match(es) not listed", n-findMaxListed)
			if got := strings.Contains(r.Summary, "older match(es) not listed"); got != (n > findMaxListed) {
				t.Errorf("older line present = %v, want %v:\n%s", got, n > findMaxListed, r.Summary)
			} else if got && !strings.Contains(r.Summary, older) {
				t.Errorf("summary lacks %q:\n%s", older, r.Summary)
			}
			if oldestShown := strings.Contains(r.Summary, "offset 0,"); oldestShown != (n <= findMaxListed) {
				t.Errorf("oldest match listed = %v, want %v (newest first, cap %d):\n%s", oldestShown, n <= findMaxListed, findMaxListed, r.Summary)
			}
			if strings.Index(r.Summary, "the scan stopped") > strings.Index(r.Summary, "\n  partition") {
				t.Errorf("truncation note comes after the list:\n%s", r.Summary)
			}
			if got := r.Data.(map[string]any)["records"].([]clients.RecordRef); len(got) != n {
				t.Errorf("data records = %d, want all %d", len(got), n)
			}
		})
	}
}

// TestFindByKeyUndecodedIsNotAMaxScanCut: #538 scenario 2. Matches at
// offsets 0 and 2 and an Avro key at 1 the registry could not decode: the
// matches are listed, the note names the undecoded key and the registry,
// and it never advises a larger max_scan, which cannot help.
func TestFindByKeyUndecodedIsNotAMaxScanCut(t *testing.T) {
	at := time.Date(2026, 10, 11, 1, 0, 0, 0, time.UTC)
	refs := []clients.RecordRef{
		{Topic: "t", Partition: 0, Offset: 0, Timestamp: at, Key: "k"},
		{Topic: "t", Partition: 0, Offset: 2, Timestamp: at.Add(time.Second), Key: "k"},
	}
	for _, tt := range []struct {
		name    string
		cut     []int32
		wantCut bool
	}{
		{"read to the end", nil, false},
		{"also cut", []int32{1}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ue := &kafka.UndecodedKeysError{
				Topic: "t", Scan: clients.KeyScan{Refs: refs, Truncated: append([]int32{0}, tt.cut...)},
				Undecoded: 1, Partitions: []int32{0}, Cut: tt.cut, Cause: errors.New("registry down"),
			}
			fk := &reportingKafka{fakeKafka: &fakeKafka{}, err: ue}
			reg := tools.NewRegistry()
			if err := Register(reg, Deps{Kafka: fk}); err != nil {
				t.Fatal(err)
			}
			tool, _ := reg.Get("kafka_find_by_key")
			r := runTool(t, tool, `{"topic":"t","key":"k","since":"2026-10-11T00:00:00Z","max_scan":10}`)
			for _, want := range []string{"found 2 record(s)", "1 key(s) in partition(s) [0] could not be decoded (schema registry unavailable)", "a larger max_scan cannot help"} {
				if !strings.Contains(r.Summary, want) {
					t.Errorf("summary %q: want %q", r.Summary, want)
				}
			}
			if strings.Contains(r.Summary, "use a later since or a larger max_scan") {
				t.Errorf("summary %q advises a larger max_scan", r.Summary)
			}
			if got := strings.Contains(r.Summary, "before the end of partition(s) [1]"); got != tt.wantCut {
				t.Errorf("summary %q: cut line present = %v, want %v", r.Summary, got, tt.wantCut)
			}
			data, _ := r.Data.(map[string]any)
			if data["truncated"] != true || data["undecoded_keys"] != 1 {
				t.Errorf("data %+v: want truncated and undecoded_keys 1", data)
			}
			if cut, _ := data["cut_partitions"].([]int32); len(cut) != len(tt.cut) {
				t.Errorf("data cut_partitions = %v, want %v", data["cut_partitions"], tt.cut)
			}
		})
	}
}

// TestFindByKeyUndecodedWithoutMatchIsAnError: with no match, undecoded keys
// stay an error (#386), never "not found".
func TestFindByKeyUndecodedWithoutMatchIsAnError(t *testing.T) {
	fk := &reportingKafka{fakeKafka: &fakeKafka{}, err: &kafka.UndecodedKeysError{Topic: "t", Undecoded: 3, Partitions: []int32{0}, Cause: errors.New("registry down")}}
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Kafka: fk}); err != nil {
		t.Fatal(err)
	}
	tool, _ := reg.Get("kafka_find_by_key")
	if _, err := tool.Run(context.Background(), json.RawMessage(`{"topic":"t","key":"k","since":"2026-10-11T00:00:00Z"}`)); err == nil {
		t.Fatal("want an error")
	}
}
