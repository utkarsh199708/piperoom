package runner

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"piperoom/internal/setup/protocol"
)

// The op log is the runner's append-only witness of every T1+ op it ran or
// refused, kept at <project dir>/.piperoom/ops.jsonl (docs/design/setup-agent.md
// §2.1). It is also the durable duplicate ledger: a (ProposalID, StepIndex)
// already started is recorded before the op runs, so the duplicate refusal
// survives a runner restart.

const (
	opLogDirName  = ".piperoom"
	opLogFileName = "ops.jsonl"

	opLogStarted = "started"
	opLogDone    = "done"
	opLogRefused = "refused"
)

// opLogEntry is one JSON line. It never carries a token, a secret value or raw
// op output: Result.Receipt holds the output MAC in their place.
type opLogEntry struct {
	ID         string             `json:"id"`
	Op         string             `json:"op"`
	Target     string             `json:"target,omitempty"`
	ProposalID string             `json:"proposal_id,omitempty"`
	StepIndex  *int               `json:"step_index,omitempty"`
	Tier       string             `json:"tier,omitempty"`
	Class      string             `json:"class,omitempty"`
	State      string             `json:"state"` // opLogStarted | opLogDone | opLogRefused
	Reason     string             `json:"reason,omitempty"`
	Result     *protocol.OpResult `json:"result,omitempty"`
	At         time.Time          `json:"at"`
}

// stepKey identifies one proposal step; it is the duplicate-refusal key.
type stepKey struct {
	proposalID string
	step       int
}

// opLog is the open append-only log file.
type opLog struct {
	f *os.File
}

// opLogPath is the log's path for the given project directory.
func opLogPath(projectDir string) string {
	return filepath.Join(projectDir, opLogDirName, opLogFileName)
}

// openOpLog creates the .piperoom directory (0700) and opens the log for
// append (O_APPEND, 0600), never truncating.
func openOpLog(projectDir string) (*opLog, error) {
	dir := filepath.Join(projectDir, opLogDirName)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("setup: creating op log directory: %w", err)
	}
	f, err := os.OpenFile(opLogPath(projectDir), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("setup: opening op log: %w", err)
	}
	return &opLog{f: f}, nil
}

// append writes one entry as a JSON line. When sync is true the file is
// fsynced before returning, so a "started" marker is durable before the op
// runs (the exactly-once ledger).
func (l *opLog) append(e opLogEntry, sync bool) error {
	raw, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("setup: encoding op log entry: %w", err)
	}
	raw = append(raw, '\n')
	if _, err := l.f.Write(raw); err != nil {
		return fmt.Errorf("setup: appending op log entry: %w", err)
	}
	if sync {
		if err := l.f.Sync(); err != nil {
			return fmt.Errorf("setup: syncing op log: %w", err)
		}
	}
	return nil
}

func (l *opLog) close() error { return l.f.Close() }

// loadStarted reads the op log and returns the set of proposal steps already
// started (state started or done). A torn or corrupt line at the tail is
// skipped: append-only writing can leave a partial last line after a crash,
// and that line was never synced, so it is safe to ignore.
func loadStarted(projectDir string) (map[stepKey]struct{}, error) {
	started := make(map[stepKey]struct{})
	f, err := os.Open(opLogPath(projectDir))
	if err != nil {
		if os.IsNotExist(err) {
			return started, nil
		}
		return nil, fmt.Errorf("setup: reading op log: %w", err)
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e opLogEntry
		if err := json.Unmarshal(line, &e); err != nil {
			continue // torn or corrupt line
		}
		if e.ProposalID == "" || e.StepIndex == nil {
			continue
		}
		if e.State == opLogStarted || e.State == opLogDone {
			started[stepKey{proposalID: e.ProposalID, step: *e.StepIndex}] = struct{}{}
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("setup: scanning op log: %w", err)
	}
	return started, nil
}
