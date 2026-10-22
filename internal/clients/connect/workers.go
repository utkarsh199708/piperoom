package connect

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"

	"piperoom/internal/clients"
)

var _ clients.WorkerTaskReader = (*Client)(nil)

// workerTasksPath is the one request WorkerTasks sends: every connector's
// status at once (Connect 2.3+, KIP-465).
const workerTasksPath = "/connectors?expand=status"

// Bounds on the free text Connect reports per task. They are names and enum
// words; anything longer or with other bytes is not passed on as is.
const (
	maxWorkerIDLen = 255
	maxStateLen    = 32
)

// expandedStatus is one value of the expand=status map. Trace fields are not
// declared, so a task's stack trace is never decoded.
type expandedStatus struct {
	Status *struct {
		Type      string `json:"type"`
		Connector struct {
			WorkerID string `json:"worker_id"`
		} `json:"connector"`
		Tasks []struct {
			ID       int    `json:"id"`
			State    string `json:"state"`
			WorkerID string `json:"worker_id"`
		} `json:"tasks"`
	} `json:"status"`
}

// WorkerTasks reads which worker runs which task for the whole Connect cluster
// with one GET /connectors?expand=status, the body capped at 4 MB, within
// clients.WorkerTasksTimeout. A Connect older than 2.3 ignores expand and
// answers the plain name list; that is clients.ErrNotSupported. It is cluster
// metadata and not filtered by pipeline: the caller reduces other pipelines'
// connectors to counts (clients.WorkerTaskReader).
func (c *Client) WorkerTasks(ctx context.Context) (map[string][]clients.TaskAssignment, error) {
	ctx, cancel := context.WithTimeout(ctx, clients.WorkerTasksTimeout)
	defer cancel()
	var raw json.RawMessage
	if err := c.do(ctx, http.MethodGet, workerTasksPath, &raw); err != nil {
		return nil, err
	}
	if t := bytes.TrimLeft(raw, " \t\r\n"); len(t) > 0 && t[0] == '[' {
		return nil, fmt.Errorf("connect: GET %s: Connect answered a name list (expand=status needs Kafka Connect 2.3+): %w", workerTasksPath, clients.ErrNotSupported)
	}
	var all map[string]expandedStatus
	if err := json.Unmarshal(raw, &all); err != nil {
		return nil, fmt.Errorf("connect: GET %s: decode: %w", workerTasksPath, err)
	}
	out := map[string][]clients.TaskAssignment{}
	for name, e := range all {
		if e.Status == nil {
			continue
		}
		typ := connectorType(e.Status.Type)
		// A worker that runs the connector instance but no task is still
		// a worker seen.
		if w := e.Status.Connector.WorkerID; w != "" {
			w = cleanWorkerID(w)
			if _, ok := out[w]; !ok {
				out[w] = []clients.TaskAssignment{}
			}
		}
		for _, t := range e.Status.Tasks {
			w := clients.UnassignedWorker
			if t.WorkerID != "" {
				w = cleanWorkerID(t.WorkerID)
			}
			out[w] = append(out[w], clients.TaskAssignment{
				Connector: name, Type: typ, Task: t.ID, State: cleanState(t.State),
			})
		}
	}
	for _, ts := range out {
		sort.Slice(ts, func(i, j int) bool {
			if ts[i].Connector != ts[j].Connector {
				return ts[i].Connector < ts[j].Connector
			}
			return ts[i].Task < ts[j].Task
		})
	}
	return out, nil
}

// connectorType keeps Connect's "source" or "sink"; anything else is "" (the
// contract's "Connect does not say").
func connectorType(t string) string {
	if t == "source" || t == "sink" {
		return t
	}
	return ""
}

// cleanState keeps a state word of upper-case ASCII letters and underscores;
// anything else is UNKNOWN, so no free text rides in a state.
func cleanState(s string) string {
	if s == "" || len(s) > maxStateLen {
		return "UNKNOWN"
	}
	for i := 0; i < len(s); i++ {
		if (s[i] < 'A' || s[i] > 'Z') && s[i] != '_' {
			return "UNKNOWN"
		}
	}
	return s
}

// cleanWorkerID keeps a worker id (host:port) of printable ASCII without
// spaces, at most 255 bytes; any other is replaced by a fixed word so it
// cannot carry text into a summary.
func cleanWorkerID(w string) string {
	if len(w) > maxWorkerIDLen {
		return "(invalid worker id)"
	}
	for i := 0; i < len(w); i++ {
		if w[i] <= ' ' || w[i] > '~' {
			return "(invalid worker id)"
		}
	}
	return w
}
