package alertin

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/connprobe"
)

// Limits of the status-page poller (docs/api.md, External changes).
const (
	// StatusFeedTimeout bounds one feed fetch.
	StatusFeedTimeout = 10 * time.Second
	// MaxStatusFeedBytes caps a feed body.
	MaxStatusFeedBytes = 4 << 20
	// MaxStatusFeedItems is how many of a feed's newest items are read.
	MaxStatusFeedItems = 50
	// StatusFeedWarnEvery is how often an unreachable feed is logged.
	StatusFeedWarnEvery = time.Hour
)

// VendorFeed is one pipeline's owners.yaml vendor_status_url.
type VendorFeed struct {
	PipelineID string
	URL        string // absolute https URL of an RSS or Atom feed
	// Component is the vendor component the rows name ("confluent-cloud",
	// "msk", "aiven-kafka"); "" derives it from the URL's host.
	Component string
}

// StatusPollerConfig configures StatusPoller.
type StatusPollerConfig struct {
	Feeds  []VendorFeed
	Writer clients.ChangeWriter
	// Client fetches the feeds; default newStatusFeedClient (public
	// addresses only, redirects to https only, StatusFeedTimeout).
	Client   *http.Client
	Interval time.Duration // default config.VendorStatusPollInterval
	Log      *slog.Logger
	Now      func() time.Time // default time.Now
}

// StatusPoller turns the active incidents of vendor status feeds (the
// statuspage.io RSS and Atom forms Confluent Cloud and Aiven publish) into
// vendor_incident rows with source statuspage. A row is one incident in one
// status: a new status of the same incident is a new row, a repeat is not.
// Every replica may run one; the store's dedup keeps one row.
type StatusPoller struct {
	cfg   StatusPollerConfig
	feeds []VendorFeed
	warn  *throttledWarn

	mu   sync.Mutex
	seen map[string]map[string]bool // pipeline → fingerprints stored or found stored
}

// NewStatusPoller checks the feeds: a valid pipeline id, an absolute https
// URL, and a component name the contract's caps allow.
func NewStatusPoller(cfg StatusPollerConfig) (*StatusPoller, error) {
	if cfg.Writer == nil {
		return nil, errors.New("alertin: status poller needs a change writer")
	}
	if cfg.Client == nil {
		cfg.Client = newStatusFeedClient()
	}
	if cfg.Interval <= 0 {
		cfg.Interval = config.VendorStatusPollInterval
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	feeds := make([]VendorFeed, 0, len(cfg.Feeds))
	for _, f := range cfg.Feeds {
		if !ValidPipelineID(f.PipelineID) {
			return nil, fmt.Errorf("alertin: vendor feed pipeline id %q is not valid", f.PipelineID)
		}
		u, err := url.Parse(f.URL)
		if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil {
			return nil, fmt.Errorf("alertin: pipeline %s: vendor_status_url must be an absolute https URL without user info", f.PipelineID)
		}
		if f.Component == "" {
			f.Component = vendorComponent(u.Hostname())
		}
		if err := (clients.ExternalChange{Service: f.Component}).Validate(); err != nil {
			return nil, fmt.Errorf("alertin: pipeline %s: vendor component: %w", f.PipelineID, err)
		}
		feeds = append(feeds, f)
	}
	return &StatusPoller{
		cfg:   cfg,
		feeds: feeds,
		warn:  &throttledWarn{every: StatusFeedWarnEvery, last: map[string]time.Time{}},
		seen:  map[string]map[string]bool{},
	}, nil
}

// newStatusFeedClient is the default feed client. Its dialer
// (connprobe.Guard with neither loopback nor private ranges allowed) refuses
// loopback, private, link-local and cloud-metadata addresses, checked on the
// resolved address it then dials, so neither the owners.yaml URL nor a
// feed's redirect can make the server reach an internal endpoint (SSRF). It
// uses no HTTP proxy: the guard would then check the proxy's address, not
// the feed's.
func newStatusFeedClient() *http.Client {
	guard := &connprobe.Guard{}
	return &http.Client{
		Timeout: StatusFeedTimeout,
		Transport: &http.Transport{
			DialContext:           guard.DialContext,
			ForceAttemptHTTP2:     true,
			TLSHandshakeTimeout:   StatusFeedTimeout,
			ResponseHeaderTimeout: StatusFeedTimeout,
			MaxIdleConns:          4,
			IdleConnTimeout:       90 * time.Second,
		},
		CheckRedirect: httpsRedirectsOnly,
	}
}

func httpsRedirectsOnly(req *http.Request, via []*http.Request) error {
	if req.URL.Scheme != "https" {
		return errors.New("alertin: status feed redirected away from https")
	}
	if len(via) >= 5 {
		return errors.New("alertin: status feed redirected too often")
	}
	return nil
}

// vendorComponent names the known vendors' status hosts the way owners.yaml
// teams[].owns and the agent name them; any other host is its own name.
func vendorComponent(host string) string {
	host = strings.ToLower(host)
	switch {
	case host == "status.confluent.cloud" || strings.HasSuffix(host, ".confluent.cloud") || strings.HasSuffix(host, ".confluent.io"):
		return "confluent-cloud"
	case strings.HasSuffix(host, ".aiven.io"):
		return "aiven-kafka"
	case strings.HasSuffix(host, ".amazon.com") || strings.HasSuffix(host, ".aws"):
		return "msk"
	}
	return host
}

// Run polls every feed at once and then every Interval until ctx ends.
func (p *StatusPoller) Run(ctx context.Context) {
	if len(p.feeds) == 0 {
		return
	}
	t := time.NewTicker(p.cfg.Interval)
	defer t.Stop()
	for {
		p.PollOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// PollOnce fetches each feed once and records its active incidents. A feed
// that cannot be fetched or read writes nothing and is logged at most once
// per StatusFeedWarnEvery.
func (p *StatusPoller) PollOnce(ctx context.Context) {
	for _, f := range p.feeds {
		if ctx.Err() != nil {
			return
		}
		p.pollFeed(ctx, f)
	}
}

func (p *StatusPoller) pollFeed(ctx context.Context, f VendorFeed) {
	now := p.cfg.Now()
	items, err := p.fetch(ctx, f.URL)
	if err != nil {
		if p.warn.due("feed:"+f.PipelineID, now) {
			p.cfg.Log.Warn("changes: vendor status feed unreachable", "pipeline", f.PipelineID, "component", f.Component, "err", err)
		}
		return
	}
	p.mu.Lock()
	prev := p.seen[f.PipelineID]
	p.mu.Unlock()
	cur := map[string]bool{}
	for _, it := range items {
		c, ok := it.change(f, now)
		if !ok {
			continue
		}
		if prev[c.Fingerprint] {
			cur[c.Fingerprint] = true
			continue
		}
		wctx, cancel := context.WithTimeout(ctx, DefaultChangeStoreTimeout)
		stored, err := p.cfg.Writer.RecordChange(wctx, c)
		cancel()
		if err != nil {
			if p.warn.due("store:"+f.PipelineID, now) {
				p.cfg.Log.Warn("changes: vendor incident not stored", "pipeline", f.PipelineID, "err", err)
			}
			continue
		}
		cur[c.Fingerprint] = true
		if stored {
			p.cfg.Log.Info("changes: vendor incident", "pipeline", f.PipelineID, "component", f.Component, "kind", clients.ChangeVendorIncident, "stored", true)
		}
	}
	// Only what the feed still lists is kept, so the set stays the feed's size.
	p.mu.Lock()
	p.seen[f.PipelineID] = cur
	p.mu.Unlock()
}

// fetch reads one feed. The error never holds the URL (it may carry a token
// in its query): a *url.Error is unwrapped to its cause.
func (p *StatusPoller) fetch(ctx context.Context, feedURL string) ([]feedItem, error) {
	ctx, cancel := context.WithTimeout(ctx, StatusFeedTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, feedURL, nil)
	if err != nil {
		return nil, errors.New("alertin: status feed: bad request")
	}
	req.Header.Set("Accept", "application/rss+xml, application/atom+xml, application/xml;q=0.9, text/xml;q=0.8")
	resp, err := p.cfg.Client.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.As(err, &ue) {
			err = ue.Err
		}
		return nil, fmt.Errorf("alertin: status feed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("alertin: status feed: HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxStatusFeedBytes+1))
	if err != nil {
		return nil, fmt.Errorf("alertin: status feed: read: %w", err)
	}
	if len(body) > MaxStatusFeedBytes {
		return nil, fmt.Errorf("alertin: status feed: body over %d bytes", MaxStatusFeedBytes)
	}
	return parseFeed(body)
}

// feedItem is one RSS item or Atom entry, reduced to what a row needs.
type feedItem struct {
	ID, Title, Link, Body string
	At                    time.Time
}

type feedDoc struct {
	Items   []rssItem   `xml:"channel>item"`
	Entries []atomEntry `xml:"entry"`
}

type rssItem struct {
	Title       string `xml:"title"`
	Link        string `xml:"link"`
	GUID        string `xml:"guid"`
	PubDate     string `xml:"pubDate"`
	Description string `xml:"description"`
}

type atomEntry struct {
	ID        string `xml:"id"`
	Title     string `xml:"title"`
	Updated   string `xml:"updated"`
	Published string `xml:"published"`
	Links     []struct {
		Href string `xml:"href,attr"`
		Rel  string `xml:"rel,attr"`
	} `xml:"link"`
	Content string `xml:"content"`
	Summary string `xml:"summary"`
}

// parseFeed reads an RSS 2.0 or Atom feed. encoding/xml expands no external
// entities, so a hostile feed cannot make it read files or the network.
func parseFeed(body []byte) ([]feedItem, error) {
	var doc feedDoc
	dec := xml.NewDecoder(bytes.NewReader(body))
	dec.Strict = false // feeds in the wild carry HTML entities like &nbsp;
	dec.Entity = xml.HTMLEntity
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("alertin: status feed: not RSS or Atom: %w", err)
	}
	var out []feedItem
	for _, it := range doc.Items {
		out = append(out, feedItem{ID: firstNonEmpty(it.GUID, it.Link), Title: it.Title, Link: strings.TrimSpace(it.Link), Body: it.Description, At: parseFeedTime(it.PubDate)})
	}
	for _, e := range doc.Entries {
		link := ""
		for _, l := range e.Links {
			if l.Rel == "" || l.Rel == "alternate" {
				link = strings.TrimSpace(l.Href)
				break
			}
		}
		out = append(out, feedItem{ID: firstNonEmpty(e.ID, link), Title: e.Title, Link: link, Body: firstNonEmpty(e.Content, e.Summary), At: parseFeedTime(firstNonEmpty(e.Updated, e.Published))})
	}
	if len(out) > MaxStatusFeedItems {
		out = out[:MaxStatusFeedItems] // feeds list the newest first
	}
	return out, nil
}

var feedTimeLayouts = []string{time.RFC3339, time.RFC1123Z, time.RFC1123, "Mon, 2 Jan 2006 15:04:05 -0700", "Mon, 2 Jan 2006 15:04:05 MST"}

func parseFeedTime(s string) time.Time {
	s = strings.TrimSpace(s)
	for _, l := range feedTimeLayouts {
		if t, err := time.Parse(l, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

// statusRe finds the update states statuspage.io puts in bold at the start of
// each update, newest first: "<strong>Monitoring</strong> - ...".
var statusRe = regexp.MustCompile(`(?i)<strong>\s*([a-z ]{1,32}?)\s*</strong>`)

// incidentStatus is the newest known state in an item's body, or "" when it
// has none (a maintenance, or a feed of another form).
func incidentStatus(body string) string {
	for _, m := range statusRe.FindAllStringSubmatch(body, 20) {
		switch s := strings.ToLower(m[1]); s {
		case clients.VendorInvestigating, clients.VendorIdentified, clients.VendorMonitoring, clients.VendorResolved:
			return s
		}
	}
	return ""
}

// change builds the row for an active incident, or false: resolved, no known
// state, older than MaxChangeAge, no id, or a link the contract refuses (the
// row is dropped, never fixed up).
func (it feedItem) change(f VendorFeed, now time.Time) (clients.PipelineChange, bool) {
	status := incidentStatus(it.Body)
	if status == "" || status == clients.VendorResolved || strings.TrimSpace(it.ID) == "" {
		return clients.PipelineChange{}, false
	}
	at := it.At
	if at.IsZero() || at.After(now) {
		at = now
	}
	if at.Before(now.Add(-MaxChangeAge)) {
		return clients.PipelineChange{}, false
	}
	ext := clients.ExternalChange{URL: it.Link, Service: f.Component, Status: status}
	if ext.Validate() != nil {
		return clients.PipelineChange{}, false
	}
	title := truncateRunes(strings.TrimSpace(cleanChangeText(it.Title)), MaxChangeTitleRunes)
	if title == "" {
		title = f.Component + " incident"
	}
	after, err := json.Marshal(ext)
	if err != nil {
		return clients.PipelineChange{}, false
	}
	return clients.PipelineChange{
		PipelineID:  f.PipelineID,
		Kind:        clients.ChangeVendorIncident,
		Object:      f.Component,
		At:          at,
		Source:      clients.ChangeSourceStatuspage,
		Fingerprint: hashParts("statuspage", f.Component, strings.TrimSpace(it.ID), status),
		After:       after,
		Summary:     title,
	}, true
}
