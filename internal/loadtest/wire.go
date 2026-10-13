package loadtest

import (
	"bytes"
	"encoding/json"
	"strconv"
	"time"
)

// frame is the part of a WebSocket server message the load clients need.
type frame struct {
	Kind string // "event", "presence", "error" or "" when unknown
	Seq  int64
	Type string    // event type
	At   time.Time // event time (server clock)
	// MarkNS is the send time a driver comment carries ("lt:<unix ns> ..."),
	// 0 when the event is not such a comment.
	MarkNS int64
}

var (
	prefixEvent    = []byte(`{"type":"event"`)
	prefixPresence = []byte(`{"type":"presence"`)
	prefixError    = []byte(`{"type":"error"`)
	keySeq         = []byte(`"seq":`)
	keyType        = []byte(`"type":"`)
	keyAt          = []byte(`"at":"`)
	keyMark        = []byte(`"text":"` + commentMark)
)

// commentMark starts the text of every driver comment, followed by the send
// time in unix nanoseconds.
const commentMark = "lt:"

// parseFrame extracts a frame without decoding the whole message. The ws
// gateway writes {"type":...,"event":{room_id,seq,type,actor,payload,at}} in
// that field order (encoding/json follows struct order), so the first "seq"
// is the event's, the "type" right after it is the event type, and the last
// "at" is the event time; anything inside the payload is escaped string data
// or comes before "at". ok is false when an event frame does not have that
// shape; callers then fall back to decodeFrame.
func parseFrame(b []byte) (f frame, ok bool) {
	switch {
	case bytes.HasPrefix(b, prefixPresence):
		return frame{Kind: "presence"}, true
	case bytes.HasPrefix(b, prefixError):
		return frame{Kind: "error"}, true
	case !bytes.HasPrefix(b, prefixEvent):
		return frame{}, false
	}
	f.Kind = "event"
	rest := b[len(prefixEvent):]
	i := bytes.Index(rest, keySeq)
	if i < 0 {
		return f, false
	}
	rest = rest[i+len(keySeq):]
	j := 0
	for j < len(rest) && rest[j] >= '0' && rest[j] <= '9' {
		j++
	}
	seq, err := strconv.ParseInt(string(rest[:j]), 10, 64)
	if err != nil {
		return f, false
	}
	f.Seq = seq
	rest = rest[j:]
	if !bytes.HasPrefix(rest, []byte(`,`+string(keyType))) {
		return f, false
	}
	rest = rest[1+len(keyType):]
	k := bytes.IndexByte(rest, '"')
	if k < 0 {
		return f, false
	}
	f.Type = string(rest[:k])
	rest = rest[k:]
	a := bytes.LastIndex(rest, keyAt)
	if a < 0 {
		return f, false
	}
	at := rest[a+len(keyAt):]
	e := bytes.IndexByte(at, '"')
	if e < 0 {
		return f, false
	}
	t, err := time.Parse(time.RFC3339Nano, string(at[:e]))
	if err != nil {
		return f, false
	}
	f.At = t
	if f.Type == "comment.added" {
		if m := bytes.Index(rest[:a], keyMark); m >= 0 {
			d := rest[m+len(keyMark):]
			n := 0
			for n < len(d) && d[n] >= '0' && d[n] <= '9' {
				n++
			}
			if v, err := strconv.ParseInt(string(d[:n]), 10, 64); err == nil {
				f.MarkNS = v
			}
		}
	}
	return f, true
}

// decodeFrame is the slow, exact path: a full JSON decode.
func decodeFrame(b []byte) (frame, error) {
	var m struct {
		Type  string `json:"type"`
		Event *struct {
			Seq     int64           `json:"seq"`
			Type    string          `json:"type"`
			Payload json.RawMessage `json:"payload"`
			At      time.Time       `json:"at"`
		} `json:"event"`
	}
	if err := json.Unmarshal(b, &m); err != nil {
		return frame{}, err
	}
	f := frame{Kind: m.Type}
	if m.Event == nil {
		return f, nil
	}
	f.Seq, f.Type, f.At = m.Event.Seq, m.Event.Type, m.Event.At
	if f.Type == "comment.added" {
		var p struct {
			Text string `json:"text"`
		}
		if json.Unmarshal(m.Event.Payload, &p) == nil && len(p.Text) > len(commentMark) && p.Text[:len(commentMark)] == commentMark {
			d := p.Text[len(commentMark):]
			n := 0
			for n < len(d) && d[n] >= '0' && d[n] <= '9' {
				n++
			}
			if v, err := strconv.ParseInt(d[:n], 10, 64); err == nil {
				f.MarkNS = v
			}
		}
	}
	return f, nil
}
