package topology

import (
	"context"
	"errors"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"piperoom/internal/clients"
)

// Schema Registry is a dependency of the connectors that serialize records
// and of the consumers that deserialize them, not a stage records pass
// through. So a subject is a side node: depends_on edges point at it from
// each connector or group that uses one of the pipeline's topics, and no data
// edge touches it. Subjects follow TopicNameStrategy: "<topic>-key" and
// "<topic>-value".

// subjectSnapshot is the Schema Registry reading of one subject.
type subjectSnapshot struct {
	versions    []int
	versionsErr error
	compat      string
	compatErr   error
}

// subjectSeen is what this builder last read for a subject. The registry API
// has no registration time, so "registered recently" can only mean "this
// builder saw the latest version change", and the first reading of a subject
// cannot tell.
type subjectSeen struct {
	version   int
	firstSeen time.Time
	changedAt time.Time // zero until a reading shows a higher version
	deletedAt time.Time // zero until a reading shows a lower version
}

// compatLevelRE accepts the registry's fixed compatibility vocabulary
// (BACKWARD, FULL_TRANSITIVE, NONE, ...). Anything else becomes "unknown", so
// a registry answer cannot put free text into the map.
var compatLevelRE = regexp.MustCompile(`^[A-Z_]{1,32}$`)

// topicUser is a connector or consumer group that writes or reads a topic.
type topicUser struct {
	id     string
	mapped bool // the data edge to or from the topic is proven
}

// topicUsers lists the writers (connectors) then the readers (connectors and
// groups) of a topic node, from the data edges.
func topicUsers(edges []Edge, topicID string) []topicUser {
	var out []topicUser
	add := func(id string, mapped bool) {
		for i := range out {
			if out[i].id == id {
				out[i].mapped = out[i].mapped || mapped
				return
			}
		}
		out = append(out, topicUser{id: id, mapped: mapped})
	}
	for _, e := range edges {
		if !e.IsDependency() && e.To == topicID && strings.HasPrefix(e.From, "connect:") {
			add(e.From, !e.IsInferred())
		}
	}
	for _, e := range edges {
		if !e.IsDependency() && e.From == topicID && (strings.HasPrefix(e.To, "connect:") || strings.HasPrefix(e.To, "group:")) {
			add(e.To, !e.IsInferred())
		}
	}
	return out
}

// regProof is what a user's own config says about one subject.
type regProof int

const (
	regUnknown regProof = iota // no config to read, or the worker's converter
	regYes                     // a registry converter under TopicNameStrategy
	regNo                      // another converter or subject naming: not this subject
)

// planSubjects picks the schema subjects of the pipeline's topics and the
// depends_on edges into them. A subject is in play when the registry lists
// it, when a user's own converter config proves it, or whenever the registry
// is unreachable: an outage must show the registry in the map as unknown,
// not hide it, so every real topic then gets both subjects. A user whose own
// config rules the subject out gets no edge. An edge is mapped only when the
// user's data edge is proven and the listing or the user's config proves the
// subject; otherwise it is inferred.
func (b *builder) planSubjects(info *collected, fp flowPlan) ([]string, []Edge) {
	if b.sr == nil {
		return nil, nil
	}
	var subjects []string
	var deps []Edge
	for _, t := range fp.topics {
		if fp.placeholder[t] {
			continue // the real topic name, and so its subject, is unknown
		}
		users := topicUsers(fp.edges, "topic:"+t)
		for _, part := range []string{"key", "value"} {
			subject := t + "-" + part
			listed := info.srErr == nil && info.srListed[subject]
			var edges []Edge
			anyProof := false
			for _, u := range users {
				proof := registryProof(info, u.id, part)
				if proof == regNo {
					continue
				}
				anyProof = anyProof || proof == regYes
				e := Edge{From: u.id, To: "sr:" + subject, Kind: EdgeDependsOn}
				if !u.mapped || !(listed || proof == regYes) {
					e = inferred(e)
				}
				edges = append(edges, e)
			}
			if len(edges) == 0 || !(listed || anyProof || info.srErr != nil) {
				continue
			}
			subjects = append(subjects, subject)
			deps = append(deps, edges...)
		}
	}
	return subjects, deps
}

// registryProof reads a connector's own converter config for part ("key" or
// "value"). Groups have no config the builder can read, and a connector that
// relies on the worker's converter says nothing either.
func registryProof(info *collected, id, part string) regProof {
	name, ok := strings.CutPrefix(id, "connect:")
	if !ok {
		return regUnknown
	}
	snap, ok := info.connect[name]
	if !ok || snap.err != nil {
		return regUnknown
	}
	conv := strings.ToLower(strings.TrimSpace(snap.config[part+".converter"]))
	switch {
	case conv == "":
		return regUnknown
	case !strings.Contains(conv, "avroconverter") && !strings.Contains(conv, "protobufconverter") && !strings.Contains(conv, "jsonschemaconverter"):
		return regNo // JsonConverter, StringConverter, ...: no registry
	}
	strategy := strings.TrimSpace(snap.config[part+".converter."+part+".subject.name.strategy"])
	if strategy == "" || strings.HasSuffix(strategy, ".TopicNameStrategy") {
		return regYes
	}
	return regNo // the subject is named some other way
}

// collectSchemas reads the latest version and compatibility of every listed
// subject in parallel. Unlisted subjects need no call: the listing already
// says they do not exist (or the registry is down and nothing is listed).
func (b *builder) collectSchemas(ctx context.Context, subjects []string, c *collected) {
	if b.sr == nil { // planSubjects returns none then; this only rules out a nil call
		return
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	for _, s := range subjects {
		if !c.srListed[s] {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			var snap subjectSnapshot
			snap.versions, snap.versionsErr = b.sr.Versions(ctx, s)
			snap.compat, _, snap.compatErr = b.sr.Compatibility(ctx, s)
			mu.Lock()
			c.subjects[s] = snap
			mu.Unlock()
		}()
	}
	wg.Wait()
}

// subjectNode builds the side node of one subject.
func (b *builder) subjectNode(subject string, info *collected, t Thresholds) Node {
	n := Node{
		ID:     "sr:" + subject,
		Kind:   KindSchemaSubject,
		Name:   subject,
		Layer:  "schema-registry",
		Owner:  b.layerOwner("schema-registry"),
		Health: HealthUnknown,
	}
	if info.srErr != nil {
		n.Facts = map[string]string{"error": "unreachable"}
		return n
	}
	snap := info.subjects[subject]
	switch {
	case snap.versionsErr != nil && !errors.Is(snap.versionsErr, clients.ErrNotFound):
		n.Facts = map[string]string{"error": "unreachable"}
		return n
	case len(snap.versions) == 0:
		// Not listed (only a converter config names it, so it was never
		// read), deleted after the listing, or empty: nothing is registered.
		n.Health = HealthWarn
		n.Facts = map[string]string{"exists": "false"}
		return n
	}
	latest := slices.Max(snap.versions)
	n.Facts = map[string]string{
		"latest_version": strconv.Itoa(latest),
		"compatibility":  "unknown",
	}
	if snap.compatErr == nil && compatLevelRE.MatchString(snap.compat) {
		n.Facts["compatibility"] = snap.compat
	}
	r := b.observeSubject(subject, latest, info.now, t.SchemaRecentWarn)
	n.Facts["registered_recently"] = r.registered
	n.Health = HealthOK
	// A new or a deleted latest version is the change to look at first, like
	// a fresh DLQ record.
	if r.registered == "true" {
		n.Facts["new_version_seen"] = HumanAge(r.newAge) + " ago"
		n.Health = HealthWarn
	}
	if r.deleted {
		n.Facts["version_deleted_seen"] = HumanAge(r.deletedAge) + " ago"
		n.Health = HealthWarn
	}
	return n
}

// subjectRecency is what observeSubject saw change within the window.
type subjectRecency struct {
	registered string        // "true", "false" or "unknown"
	newAge     time.Duration // since the new version was seen, when registered is "true"
	deleted    bool          // the latest version went down within the window
	deletedAge time.Duration
}

// observeSubject records a reading of a subject's latest version. registered
// is "true" when this builder saw the version go up less than window ago,
// "false" when it has watched the subject for at least window without that,
// "unknown" otherwise. A version going down is a deletion, never a
// registration: it is reported as deleted instead.
func (b *builder) observeSubject(subject string, version int, now time.Time, window time.Duration) subjectRecency {
	b.srMu.Lock()
	defer b.srMu.Unlock()
	if b.subjects == nil {
		b.subjects = map[string]subjectSeen{}
	}
	s, ok := b.subjects[subject]
	if !ok {
		b.subjects[subject] = subjectSeen{version: version, firstSeen: now}
		return subjectRecency{registered: "unknown"}
	}
	switch {
	case version > s.version:
		s.changedAt = now
	case version < s.version:
		s.deletedAt = now
	}
	s.version = version
	b.subjects[subject] = s

	var r subjectRecency
	switch {
	case !s.changedAt.IsZero() && now.Sub(s.changedAt) < window:
		r.registered, r.newAge = "true", now.Sub(s.changedAt)
	case now.Sub(s.firstSeen) >= window:
		r.registered = "false"
	default:
		r.registered = "unknown"
	}
	if !s.deletedAt.IsZero() && now.Sub(s.deletedAt) < window {
		r.deleted, r.deletedAge = true, now.Sub(s.deletedAt)
	}
	return r
}
