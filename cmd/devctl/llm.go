package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	// ollamaBaseURL is the host side of the `llm` profile's published port.
	ollamaBaseURL = "http://127.0.0.1:11434"
	// llmModel is the model llm-up pulls. llama3.1:8b is documented by Ollama
	// as tool-capable and has a 131072-token window, which is what the
	// profile's OLLAMA_CONTEXT_LENGTH sets; qwen3:8b natively stops at 40960,
	// under the 65536 floor of docs/design/model-providers.md §6.2.
	llmModel = "llama3.1:8b"
	// llmPullTimeout bounds the whole pull: a first download is about 5 GB.
	llmPullTimeout = 30 * time.Minute
	// llmMaxLine caps one progress line of the pull stream.
	llmMaxLine = 1 << 20
)

// llmUp starts the llm compose profile and pulls model into it.
func (s *stack) llmUp(ctx context.Context, baseURL, model string, pullTimeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, pullTimeout)
	defer cancel()
	if err := s.compose(ctx, nil, "--profile", "llm", "up", "-d", "--wait", "ollama"); err != nil {
		return err
	}
	s.log.Info("devctl: pulling model (first pull downloads several GB)", "model", model)
	if err := ollamaPull(ctx, s.llmClient(), baseURL, model, func(status string) {
		s.log.Debug("devctl: ollama pull", "status", status)
	}); err != nil {
		return err
	}
	s.log.Info("devctl: model ready", "model", model, "endpoint", baseURL+"/v1")
	return nil
}

// llmClient is the one HTTP client for the pull. It has no overall timeout of
// its own: a pull streams for minutes, so the caller's context is the deadline.
func (s *stack) llmClient() *http.Client {
	if s.llmHTTP != nil {
		return s.llmHTTP
	}
	return &http.Client{}
}

// ollamaPull calls POST /api/pull and reads the progress stream (one JSON
// object per line) until it reports "success" or an error.
func ollamaPull(ctx context.Context, c *http.Client, baseURL, model string, progress func(status string)) error {
	body, err := json.Marshal(map[string]any{"model": model, "stream": true})
	if err != nil {
		return fmt.Errorf("devctl: encode pull request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+"/api/pull", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("devctl: build pull request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.Do(req)
	if err != nil {
		return fmt.Errorf("devctl: ollama pull %s: %w", model, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("devctl: ollama pull %s: status %d: %s", model, resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	// Each line is capped by the scanner buffer; the overall cap is generous
	// because a multi-GB pull emits tens of thousands of progress lines.
	sc := bufio.NewScanner(io.LimitReader(resp.Body, 256<<20))
	sc.Buffer(make([]byte, 0, 64<<10), llmMaxLine)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var ev struct {
			Status string `json:"status"`
			Error  string `json:"error"`
		}
		if err := json.Unmarshal(line, &ev); err != nil {
			return fmt.Errorf("devctl: ollama pull %s: bad progress line: %w", model, err)
		}
		if ev.Error != "" {
			return fmt.Errorf("devctl: ollama pull %s: %s", model, ev.Error)
		}
		if progress != nil {
			progress(ev.Status)
		}
		if ev.Status == "success" {
			return nil
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("devctl: ollama pull %s: read stream: %w", model, err)
	}
	if ctx.Err() != nil {
		return fmt.Errorf("devctl: ollama pull %s: %w", model, ctx.Err())
	}
	return errors.New("devctl: ollama pull " + model + ": stream ended before success")
}
