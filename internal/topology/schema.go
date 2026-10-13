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
	changedAt time.Time // zero until a reading shows a new version
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

// planSubjects picks the schema subjects of the pipeline's topics and the
// depends_on edges into them. A subject is in play when the registry lists
// it, when a user's own converter config proves it, or, while the registry
// is unreachable, when this builder saw it before (so an outage keeps the
// node, as unknown, instead of dropping it). An edge is mapped only when the
// user's data edge is proven and the registry listing or the user's config
// proves the subject; otherwise it is inferred.
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
		// Every CDC topic has at least its writer, so users is never empty.
		users := topicUsers(fp.edges, "topic:"+t)
		for _, part := range []string{"key", "value"} {
			subject := t + "-" + part
			listed := info.srErr == nil && info.srListed[subject]
			proofs := make([]bool, len(users))
			anyProof := false
			for i, u := range users {
				proofs[i] = usesRegistry(info, u.id, part)
				anyProof = anyProof || proofs[i]
			}
			known := info.srErr != nil && b.seenSubject(subject)
			if !listed && !anyProof && !known {
				continue
			}
			subjects = append(subjects, subject)
			for i, u := range users {
				e := Edge{From: u.id, To: "sr:" + subject, Kind: EdgeDependsOn}
				if !u.mapped || !(listed || proofs[i]) {
					e = inferred(e)
				}
				deps = append(deps, e)
			}
		}
	}
	return subjects, deps
}

// usesRegistry reports whether a connector's own config proves it serializes
// (or deserializes) part ("key" or "value") with the registry under
// TopicNameStrategy. Groups have no config the builder can read, and a
// connector that relies on the worker's converter proves nothing either.
func usesRegistry(info *collected, id, part string) bool {
	name, ok := strings.CutPrefix(id, "connect:")
	if !ok {
		return false
	}
	snap, ok := info.connect[name]
	if !ok || snap.err != nil {
		return false
	}
	conv := strings.ToLower(strings.TrimSpace(snap.config[part+".converter"]))
	if !strings.Contains(conv, "avroconverter") && !strings.Contains(conv, "protobufconverter") && !strings.Contains(conv, "jsonschemaconverter") {
		return false
	}
	strategy := strings.TrimSpace(snap.config[part+".converter."+part+".subject.name.strategy"])
	return strategy == "" || strings.HasSuffix(strategy, ".TopicNameStrategy")
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
	recent, age := b.observeSubject(subject, latest, info.now, t.SchemaRecentWarn)
	n.Facts["registered_recently"] = recent
	n.Health = HealthOK
	if recent == "true" {
		// A new version is the change to look at first, like a fresh DLQ record.
		n.Facts["new_version_seen"] = HumanAge(age) + " ago"
		n.Health = HealthWarn
	}
	return n
}

// observeSubject records a reading of a subject's latest version and says
// whether a new version was registered within window: "true" when this
// builder saw the version change less than window ago (age is how long ago),
// "false" when it has watched the subject for at least window without a
// change in it, "unknown" otherwise.
func (b *builder) observeSubject(subject string, version int, now time.Time, window time.Duration) (string, time.Duration) {
	b.srMu.Lock()
	defer b.srMu.Unlock()
	if b.subjects == nil {
		b.subjects = map[string]subjectSeen{}
	}
	s, ok := b.subjects[subject]
	if !ok {
		b.subjects[subject] = subjectSeen{version: version, firstSeen: now}
		return "unknown", 0
	}
	if s.version != version {
		s.version = version
		s.changedAt = now
		b.subjects[subject] = s
	}
	switch {
	case !s.changedAt.IsZero() && now.Sub(s.changedAt) < window:
		return "true", now.Sub(s.changedAt)
	case now.Sub(s.firstSeen) >= window:
		return "false", 0
	default:
		return "unknown", 0
	}
}

// seenSubject reports whether this builder has read the subject before.
func (b *builder) seenSubject(subject string) bool {
	b.srMu.Lock()
	defer b.srMu.Unlock()
	_, ok := b.subjects[subject]
	return ok
}
