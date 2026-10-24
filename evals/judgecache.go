package evals

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
)

// The recorded judge verdict cache (#454). A replay or a unit test must grade
// an ambiguous hypothesis the same way every time and without a paid call, so
// the judge's verdict on each corpus text is recorded once (evalrun
// -judge-fill, an owner-approved paid run) and read back from here.
//
// One file per verdict, named by its key:
//
//	sha256(JudgePromptVersion "\x00" scenario name "\x00" hypothesis text)
//
// Bumping JudgePromptVersion therefore invalidates every entry. An entry also
// records a hash of the scenario's ground truth as the judge saw it
// (judgeTruthSHA); an entry whose scenario truth has changed since is stale,
// and reading it is an error, never a verdict on old truth.
//
// The repository is public: only synthetic texts, the eval scenarios' and the
// committed corpora's, may be cached. Nothing in a run writes here; only the
// fill command does, from the committed ambiguous-set manifest.

// DefaultJudgeCacheDir is the committed cache, relative to the repository root.
const DefaultJudgeCacheDir = "evals/judgecache"

// maxJudgeCacheEntryBytes bounds one entry file; a verdict is a few hundred
// bytes, a corpus text a few hundred more.
const maxJudgeCacheEntryBytes = 64 << 10

// ErrJudgeCacheMiss is returned when a cache-only judge has no verdict for a
// hypothesis. It fails the run: a miss is never graded by the keyword pass.
var ErrJudgeCacheMiss = errors.New("evals: no recorded judge verdict")

// ErrJudgeCacheStale is returned for an entry recorded against other ground
// truth than the scenario has now.
var ErrJudgeCacheStale = errors.New("evals: recorded judge verdict is stale")

// JudgeCacheEntry is one recorded verdict.
type JudgeCacheEntry struct {
	PromptVersion string `json:"prompt_version"`
	Scenario      string `json:"scenario"`
	Hypothesis    string `json:"hypothesis"`
	// TruthSHA256 is judgeTruthSHA of the scenario when the verdict was
	// recorded.
	TruthSHA256 string `json:"truth_sha256"`
	// Model is the judge that gave the verdict, as "provider:model"
	// (JudgeID; an entry recorded before V9 holds the bare model). It is not
	// part of the key: a verdict stays valid across models and providers, and
	// the report says which judges' verdicts it read.
	Model   string  `json:"model,omitempty"`
	Verdict Verdict `json:"verdict"`
}

// JudgeCache reads and writes recorded verdicts in one directory.
type JudgeCache struct {
	dir string
}

// NewJudgeCache returns the cache in dir. The directory need not exist until
// the first Put.
func NewJudgeCache(dir string) *JudgeCache {
	return &JudgeCache{dir: dir}
}

// Dir is the cache's directory.
func (c *JudgeCache) Dir() string { return c.dir }

// JudgeCacheKey is the key of one verdict (see the file comment).
func JudgeCacheKey(promptVersion, scenario, hypothesis string) string {
	h := sha256.New()
	io.WriteString(h, promptVersion)
	h.Write([]byte{0})
	io.WriteString(h, scenario)
	h.Write([]byte{0})
	io.WriteString(h, hypothesis)
	return hex.EncodeToString(h.Sum(nil))
}

// judgeTruthSHA hashes the ground truth the judge is shown for sc: the prompt
// with an empty hypothesis.
func judgeTruthSHA(sc Scenario) string {
	sum := sha256.Sum256([]byte(judgeSystem + "\x00" + judgePrompt(sc, "")))
	return hex.EncodeToString(sum[:])
}

var cacheKeyFile = regexp.MustCompile(`^[0-9a-f]{64}\.json$`)

func (c *JudgeCache) path(key string) string {
	return filepath.Join(c.dir, key+".json")
}

// Get returns the recorded verdict for hypothesis under sc. ok is false when
// there is none. A stale or malformed entry is an error.
func (c *JudgeCache) Get(sc Scenario, hypothesis string) (Verdict, bool, error) {
	e, ok, err := c.Lookup(sc, hypothesis)
	return e.Verdict, ok, err
}

// Lookup is Get with the whole entry (the judge model too).
func (c *JudgeCache) Lookup(sc Scenario, hypothesis string) (JudgeCacheEntry, bool, error) {
	key := JudgeCacheKey(JudgePromptVersion, sc.Name, hypothesis)
	f, err := os.Open(c.path(key))
	if errors.Is(err, fs.ErrNotExist) {
		return JudgeCacheEntry{}, false, nil
	}
	if err != nil {
		return JudgeCacheEntry{}, false, fmt.Errorf("evals: read judge cache: %w", err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxJudgeCacheEntryBytes+1))
	if err != nil {
		return JudgeCacheEntry{}, false, fmt.Errorf("evals: read judge cache entry %s: %w", key, err)
	}
	if len(b) > maxJudgeCacheEntryBytes {
		return JudgeCacheEntry{}, false, fmt.Errorf("evals: judge cache entry %s is over %d bytes", key, maxJudgeCacheEntryBytes)
	}
	var e JudgeCacheEntry
	if err := json.Unmarshal(b, &e); err != nil {
		return JudgeCacheEntry{}, false, fmt.Errorf("evals: parse judge cache entry %s: %w", key, err)
	}
	// The key already pins all three; a mismatch is a hand-edited or
	// misnamed file.
	if e.PromptVersion != JudgePromptVersion || e.Scenario != sc.Name || e.Hypothesis != hypothesis {
		return JudgeCacheEntry{}, false, fmt.Errorf("evals: judge cache entry %s does not match its key", key)
	}
	if e.TruthSHA256 != judgeTruthSHA(sc) {
		return JudgeCacheEntry{}, false, fmt.Errorf("%w: entry %s for scenario %q was recorded against other ground truth (description, layers, root_cause or root_cause_forbidden changed); refill it with evalrun -judge-fill", ErrJudgeCacheStale, key, sc.Name)
	}
	return e, true, nil
}

// Put records v, given by the judge model ("provider:model", JudgeID; "" when
// unknown), for
// hypothesis under sc, replacing any earlier entry. The write goes through a
// temporary file, so a reader never sees half an entry.
func (c *JudgeCache) Put(sc Scenario, hypothesis, model string, v Verdict) error {
	e := JudgeCacheEntry{PromptVersion: JudgePromptVersion, Scenario: sc.Name, Hypothesis: hypothesis,
		TruthSHA256: judgeTruthSHA(sc), Model: model, Verdict: v}
	b, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		return fmt.Errorf("evals: encode judge cache entry: %w", err)
	}
	return c.write(JudgeCacheKey(JudgePromptVersion, sc.Name, hypothesis), b)
}

// write stores one encoded entry under key.
func (c *JudgeCache) write(key string, b []byte) error {
	if err := os.MkdirAll(c.dir, 0o755); err != nil {
		return fmt.Errorf("evals: create judge cache: %w", err)
	}
	tmp, err := os.CreateTemp(c.dir, ".put-*")
	if err != nil {
		return fmt.Errorf("evals: write judge cache entry: %w", err)
	}
	_, werr := tmp.Write(append(b, '\n'))
	cerr := tmp.Close()
	if err := errors.Join(werr, cerr); err != nil {
		os.Remove(tmp.Name())
		return fmt.Errorf("evals: write judge cache entry %s: %w", key, err)
	}
	if err := os.Rename(tmp.Name(), c.path(key)); err != nil {
		os.Remove(tmp.Name())
		return fmt.Errorf("evals: write judge cache entry %s: %w", key, err)
	}
	return nil
}

// Len counts the entries in the cache (files named by a key); a missing
// directory holds none.
func (c *JudgeCache) Len() (int, error) {
	des, err := os.ReadDir(c.dir)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("evals: list judge cache: %w", err)
	}
	n := 0
	for _, d := range des {
		if !d.IsDir() && cacheKeyFile.MatchString(d.Name()) {
			n++
		}
	}
	return n, nil
}
