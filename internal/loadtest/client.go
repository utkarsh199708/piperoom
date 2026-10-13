package loadtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/events"
)

// Users the driver acts as (room.DefaultUsers). The first one creates rooms
// and so is their driver.
var loadUsers = []string{"arjun", "priya", "meera", "kabir"}

// API is a small client for the room HTTP API.
type API struct {
	Base string
	HTTP *http.Client
}

// newHTTPClient returns the driver's shared HTTP client.
func newHTTPClient() *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = 0
	tr.MaxIdleConnsPerHost = 512
	return &http.Client{Transport: tr, Timeout: 60 * time.Second}
}

// apiError is a non-2xx answer.
type apiError struct {
	Method, Path string
	Status       int
	Body         string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("loadtest: %s %s: status %d: %s", e.Method, e.Path, e.Status, e.Body)
}

func (a *API) do(ctx context.Context, method, path, user string, body any, dst any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("loadtest: %s %s: %w", method, path, err)
		}
		rd = strings.NewReader(string(b))
	}
	req, err := http.NewRequestWithContext(ctx, method, a.Base+path, rd)
	if err != nil {
		return fmt.Errorf("loadtest: %s %s: %w", method, path, err)
	}
	req.Header.Set("X-User", user)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := a.HTTP.Do(req)
	if err != nil {
		return fmt.Errorf("loadtest: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	lr := io.LimitReader(resp.Body, 4<<20)
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(lr, 512))
		return &apiError{Method: method, Path: path, Status: resp.StatusCode, Body: strings.TrimSpace(string(b))}
	}
	if dst == nil {
		_, _ = io.Copy(io.Discard, lr)
		return nil
	}
	if err := json.NewDecoder(lr).Decode(dst); err != nil {
		return fmt.Errorf("loadtest: %s %s: decode: %w", method, path, err)
	}
	return nil
}

// CreateRoom opens a room as user (who becomes its driver) and returns its id.
func (a *API) CreateRoom(ctx context.Context, user, title, pipeline string) (string, error) {
	var snap struct {
		ID string `json:"id"`
	}
	err := a.do(ctx, http.MethodPost, "/api/rooms", user, map[string]string{
		"title": title, "severity": "sev2", "pipeline_id": pipeline,
	}, &snap)
	if err != nil {
		return "", err
	}
	if snap.ID == "" {
		return "", errors.New("loadtest: create room: empty id")
	}
	return snap.ID, nil
}

// Comment posts a comment whose text carries the send time, for end-to-end
// latency, padded to pad bytes.
func (a *API) Comment(ctx context.Context, user, roomID string, pad int) error {
	text := commentMark + strconv.FormatInt(time.Now().UnixNano(), 10) + " " + fillerText(pad)
	return a.do(ctx, http.MethodPost, "/api/rooms/"+roomID+"/comments", user, map[string]string{"text": text}, nil)
}

// Steer sends a steer (the driver's sends steer.sent).
func (a *API) Steer(ctx context.Context, user, roomID, text string) error {
	return a.do(ctx, http.MethodPost, "/api/rooms/"+roomID+"/steer", user, map[string]string{"text": text}, nil)
}

// CloseRoom closes a room as its driver.
func (a *API) CloseRoom(ctx context.Context, user, roomID string) error {
	return a.do(ctx, http.MethodPost, "/api/rooms/"+roomID+"/close", user, map[string]string{"resolution": "load test done"}, nil)
}

// Events reads a room's whole log.
func (a *API) Events(ctx context.Context, user, roomID string) ([]events.Event, error) {
	var all []events.Event
	after := int64(0)
	for {
		var page struct {
			Events  []events.Event `json:"events"`
			LastSeq int64          `json:"last_seq"`
		}
		if err := a.do(ctx, http.MethodGet, fmt.Sprintf("/api/rooms/%s/events?after=%d&limit=200", roomID, after), user, nil, &page); err != nil {
			return nil, err
		}
		all = append(all, page.Events...)
		if len(page.Events) == 0 || page.Events[len(page.Events)-1].Seq >= page.LastSeq {
			return all, nil
		}
		after = page.Events[len(page.Events)-1].Seq
	}
}

// LastSeq returns a room's last seq.
func (a *API) LastSeq(ctx context.Context, user, roomID string) (int64, error) {
	var page struct {
		LastSeq int64 `json:"last_seq"`
	}
	err := a.do(ctx, http.MethodGet, fmt.Sprintf("/api/rooms/%s/events?after=0&limit=1", roomID), user, nil, &page)
	return page.LastSeq, err
}

// LatencySink receives delivery latencies from subscribers. Nil fields are
// not recorded.
type LatencySink struct {
	Delivery *Recorder // receive time minus the event's server "at"
	EndToEnd *Recorder // receive time minus a driver comment's send time
	// From drops events whose "at" is before it (warm-up, replay).
	From atomic.Int64 // unix ns
	// Off stops recording when set.
	Off atomic.Bool
}

// Sub is one WebSocket subscriber. Its counters are written by its own
// goroutine and read after Run returns, or through the atomics while it runs.
type Sub struct {
	Room   string
	User   string
	Sink   *LatencySink
	Target int64 // close Done once LastSeq reaches it (0 = never)
	// Reconnect makes the subscriber reconnect with after=LastSeq when the
	// server closes it, as the web UI does.
	Reconnect bool

	Done chan struct{} // closed when LastSeq >= Target, or when Run ends

	LastSeq    atomic.Int64
	Received   int64
	Bytes      atomic.Int64
	Gaps       int64
	Dups       int64
	Reconnects int
	DialErrors int
	Mismatches int64 // fast parse disagreed with the full decode
	Closes     map[string]int
	Started    time.Time
	DoneAt     time.Time
	Connected  atomic.Bool

	doneOnce sync.Once
}

// NewSub returns a subscriber for roomID.
func NewSub(roomID, user string, target int64, sink *LatencySink) *Sub {
	return &Sub{Room: roomID, User: user, Target: target, Sink: sink, Done: make(chan struct{}), Closes: map[string]int{}}
}

func (s *Sub) finish() {
	s.doneOnce.Do(func() {
		s.DoneAt = time.Now()
		close(s.Done)
	})
}

// closeKey names how a connection ended, for the drop counters.
func closeKey(err error) string {
	var ce websocket.CloseError
	if errors.As(err, &ce) {
		return fmt.Sprintf("%d %s", ce.Code, ce.Reason)
	}
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return ""
	case errors.Is(err, io.EOF):
		return "eof"
	}
	return "error"
}

// Run connects and reads until ctx ends (or the server closes and Reconnect
// is off). base is the server's http:// URL.
func (s *Sub) Run(ctx context.Context, base string, client *http.Client) {
	defer s.finish()
	s.Started = time.Now()
	wsBase := "ws" + strings.TrimPrefix(base, "http")
	for ctx.Err() == nil {
		u := fmt.Sprintf("%s/api/rooms/%s/ws?after=%d", wsBase, s.Room, s.LastSeq.Load())
		conn, _, err := websocket.Dial(ctx, u, &websocket.DialOptions{
			HTTPClient: client,
			HTTPHeader: http.Header{"X-User": []string{s.User}},
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.DialErrors++
			if !s.Reconnect || s.DialErrors > 20 {
				return
			}
			sleepCtx(ctx, 200*time.Millisecond)
			continue
		}
		conn.SetReadLimit(4 << 20)
		s.Connected.Store(true)
		err = s.read(ctx, conn)
		s.Connected.Store(false)
		_ = conn.CloseNow()
		if ctx.Err() != nil {
			return
		}
		if k := closeKey(err); k != "" {
			s.Closes[k]++
		}
		if !s.Reconnect {
			return
		}
		s.Reconnects++
	}
}

func (s *Sub) read(ctx context.Context, conn *websocket.Conn) error {
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Presence every 20 s keeps the server's 60 s read deadline from closing us.
	go func() {
		t := time.NewTicker(20 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-cctx.Done():
				return
			case <-t.C:
				wctx, wcancel := context.WithTimeout(cctx, 5*time.Second)
				_ = conn.Write(wctx, websocket.MessageText, []byte(`{"type":"presence","state":"viewing"}`))
				wcancel()
			}
		}
	}()
	var n int64
	for {
		_, data, err := conn.Read(cctx)
		if err != nil {
			return err
		}
		now := time.Now()
		s.Bytes.Add(int64(len(data)))
		f, ok := parseFrame(data)
		n++
		// Every 512th frame (and any the fast path refuses) is fully decoded,
		// and a disagreement is counted: the run reports it as a check.
		if !ok || n%512 == 0 {
			slow, err := decodeFrame(data)
			if err != nil {
				s.Mismatches++
				continue
			}
			if ok && (slow.Kind != f.Kind || slow.Seq != f.Seq || slow.Type != f.Type || !slow.At.Equal(f.At) || slow.MarkNS != f.MarkNS) {
				s.Mismatches++
			}
			f = slow
		}
		if f.Kind != "event" {
			continue
		}
		s.Received++
		last := s.LastSeq.Load()
		switch {
		case f.Seq == last+1:
		case f.Seq <= last:
			s.Dups++
			continue
		default:
			s.Gaps += f.Seq - last - 1
		}
		s.LastSeq.Store(f.Seq)
		if sk := s.Sink; sk != nil && !sk.Off.Load() && f.At.UnixNano() >= sk.From.Load() {
			if sk.Delivery != nil {
				sk.Delivery.Add(now.Sub(f.At))
			}
			if sk.EndToEnd != nil && f.MarkNS > 0 {
				sk.EndToEnd.Add(now.Sub(time.Unix(0, f.MarkNS)))
			}
		}
		if s.Target > 0 && f.Seq >= s.Target {
			s.finish()
		}
	}
}

// SubTotals sums a set of subscribers.
type SubTotals struct {
	Subs        int            `json:"subs"`
	Received    int64          `json:"received"`
	Bytes       int64          `json:"bytes"`
	Gaps        int64          `json:"gaps"`
	Dups        int64          `json:"dups"`
	Reconnects  int            `json:"reconnects"`
	DialErrors  int            `json:"dial_errors"`
	Mismatches  int64          `json:"parse_mismatches"`
	Closes      map[string]int `json:"server_closes"`
	SlowClosed  int            `json:"send_buffer_full_closes"`
	MissingTail int64          `json:"missing_at_end"`
}

// Totals sums subs; final maps a room to its last seq for the missing-tail
// count (nil skips it).
func Totals(subs []*Sub, final map[string]int64) SubTotals {
	t := SubTotals{Subs: len(subs), Closes: map[string]int{}}
	for _, s := range subs {
		t.Received += s.Received
		t.Bytes += s.Bytes.Load()
		t.Gaps += s.Gaps
		t.Dups += s.Dups
		t.Reconnects += s.Reconnects
		t.DialErrors += s.DialErrors
		t.Mismatches += s.Mismatches
		for k, v := range s.Closes {
			t.Closes[k] += v
			if strings.Contains(k, "send buffer full") {
				t.SlowClosed += v
			}
		}
		if final != nil {
			if d := final[s.Room] - s.LastSeq.Load(); d > 0 {
				t.MissingTail += d
			}
		}
	}
	return t
}
