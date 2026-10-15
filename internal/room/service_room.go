package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/store"
)

var severities = map[string]bool{"sev1": true, "sev2": true, "sev3": true, "sev4": true}

// pipelineIDRe is the accepted shape of pipeline_id on POST /api/rooms. The
// id flows into logs, prompts and identifier-based lookups, so only a safe
// charset may pass (C13 prompt audit, item (b)).
var pipelineIDRe = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)

// CreateRoom opens a room; the creator becomes driver and the agent starts.
func (s *Service) CreateRoom(ctx context.Context, user User, title, severity, pipelineID string) (RoomSnapshot, error) {
	if title == "" || pipelineID == "" {
		return RoomSnapshot{}, validation("title and pipeline_id are required")
	}
	// The title is client text stored in a Postgres text column and in jsonb.
	if err := rejectUnstorable(title); err != nil {
		return RoomSnapshot{}, err
	}
	if err := checkLen("title", title, MaxTitleChars); err != nil {
		return RoomSnapshot{}, err
	}
	// The shape check applies even without an owners config (demo, unit
	// tests): it does not depend on anything being loaded.
	if !pipelineIDRe.MatchString(pipelineID) {
		return RoomSnapshot{}, validation(fmt.Sprintf("pipeline_id %q must match ^[a-zA-Z0-9_.-]+$", pipelineID))
	}
	if !severities[severity] {
		return RoomSnapshot{}, validation("severity must be sev1..sev4")
	}
	// With an owners config loaded, the pipeline must be known (docs/api.md:
	// unknown pipeline on POST /api/rooms → 400 validation). Without one
	// (--demo, unit tests) there is nothing to validate against.
	// A pipeline outside the person's tenants gets the same answer, so its
	// existence does not leak (docs/design/tenancy.md section 6).
	if s.owners != nil {
		if _, ok := s.owners.Pipeline(pipelineID); !ok || !s.CanSeePipeline(user, pipelineID) {
			return RoomSnapshot{}, validation(fmt.Sprintf("unknown pipeline %q", pipelineID))
		}
	}
	roomID := NewID("r_")
	now := s.now().UTC()
	lease := now.Add(DriverLease)

	if err := s.store.CreateRoom(ctx, store.Room{
		ID: roomID, Title: title, Severity: severity, PipelineID: pipelineID, Status: "open", OpenedAt: now,
	}); err != nil {
		return RoomSnapshot{}, fmt.Errorf("room: create: %w", err)
	}
	st := &roomState{snap: NewSnapshot(), inbox: newInbox(), openingFor: pipelineID}
	st.snap.ID = roomID
	// Locked before it is cached: nobody sees the room before room.created.
	s.lockState(st)
	s.cacheRoom(roomID, st)

	if _, err := s.emitLocked(ctx, st, roomID, events.RoomCreated, s.humanActor(user), events.RoomCreatedPayload{
		Title: title, Severity: severity, PipelineID: pipelineID,
	}); err != nil {
		s.discardNewRoomLocked(ctx, roomID, st)
		s.unlockRoom(st)
		return RoomSnapshot{}, mapUnstorable(err)
	}
	// room.created is logged, so the room exists (afterAppend): a failed
	// grant leaves it without a driver, like an alert room, and anyone may
	// take control. An error here would make the client open a second room.
	if _, err := s.emitLocked(ctx, st, roomID, events.ControlGranted, s.humanActor(user), events.ControlGrantedPayload{
		To: s.humanActor(user), LeaseUntil: lease,
	}); err != nil {
		s.afterAppend(roomID, "grant control to the room's creator", err, "user", user.ID)
	} else {
		s.afterAppend(roomID, "persist creator driver", s.store.UpdateDriver(ctx, roomID, user.ID, user.Name, lease))
	}
	snap := st.snap.Clone()
	st.firstLook = true
	s.startAgentLocked(roomID, title, pipelineID, nil, st)
	s.notifyRoomOpenedLocked(roomID)
	s.unlockRoom(st)
	return snap, nil
}

// discardNewRoomLocked undoes a room whose room.created append failed: the
// cache entry goes and the rooms row is deleted (DeleteEmptyRoom only deletes
// a row with no events), so no orphan row is left behind that would load as a
// room with an empty log. The delete uses a fresh deadline because ctx may be
// the reason the append failed. If the delete fails too, Load still skips the
// empty row (loadRows).
//
// An append error does not prove the event was not stored: a connection that
// drops after the commit reports an error for a committed event. Then
// DeleteEmptyRoom finds last_seq > 0 and keeps the room (ErrRoomNotEmpty is
// only logged), and the next Load, ListRooms or GetRoom rebuilds it from its
// log (those load any room that is not cached). The caller got an
// error for a room that exists; a retry of an alert joins it by its dedup key.
// Caller holds st's lock.
func (s *Service) discardNewRoomLocked(ctx context.Context, roomID string, st *roomState) {
	st.gone = true
	s.uncacheRoom(roomID, st)
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := s.store.DeleteEmptyRoom(dctx, roomID); err != nil {
		s.log.Warn("room: remove room row after failed room.created", "room", roomID, "err", err)
	}
}

// CreateRoomFromAlert handles the generic alert webhook (POST
// /api/alerts/webhook): the caller has mapped title/severity/pipeline_id from
// the body, and the dedup key and resolved flag are read from the raw body by
// alertDedup. It builds an alertin.Alert from those and opens it through the
// same code as OpenAlertRoom, recorded as source "webhook". Its dedup key gets
// the webhook prefix, so an X-User caller can never hold or join the key of a
// signed source, even with an identical body. created is false when the alert
// joined the open room for it instead of opening one.
func (s *Service) CreateRoomFromAlert(ctx context.Context, title, severity, pipelineID string, raw json.RawMessage) (RoomSnapshot, bool, error) {
	dedupKey, resolved := alertDedup(raw)
	status := alertin.StatusFiring
	if resolved {
		status = alertin.StatusResolved
	}
	if dedupKey != "" {
		dedupKey = webhookKeyPrefix + dedupKey
	}
	return s.openAlertRoom(ctx, alertin.Alert{
		Source:     alertin.Generic,
		Status:     status,
		Title:      title,
		Severity:   severity,
		PipelineID: pipelineID,
		DedupKey:   dedupKey,
		Raw:        raw,
	}, alertSourceWebhook)
}

// alertSourceWebhook is room.created.alert_source for the dev X-User webhook.
// Signed rooms record the alertin source name instead.
const alertSourceWebhook = "webhook"

// webhookKeyPrefix separates the dev webhook's dedup keys from every signed
// source's ("alertmanager:...", "generic:..."), which alertin builds itself.
const webhookKeyPrefix = "webhook:"

// OpenAlertRoom opens a room for a, or joins the open room that already holds
// a.DedupKey for the same pipeline. It is the single entry point for every alert
// source: the dev webhook goes through CreateRoomFromAlert, the signed routes
// pass the alertin.Alert they verified. It maps a.Title/Severity/PipelineID
// and stores a.Raw verbatim in room.created.alert. The pipeline id takes the
// same shape check as POST /api/rooms, but a missing or unknown pipeline NEVER
// drops the alert (docs/api.md:103, LEAD-ARCH round 1 #10): the room opens
// anyway with a system message and proposals disabled, so the agent cannot
// propose a fix against a pipeline it knows nothing about. The 400 for an
// unknown pipeline stays only on POST /api/rooms (CreateRoom). With no owners
// config (unit tests) there is nothing to validate against, so every pipeline
// is treated as known. No driver until someone takes control. created is false
// when the alert joined the open room for it (see alertIndexKey) instead of
// opening one. A resolved alert (a.Status) that matches no open room returns
// ErrResolvedAlertNoRoom and changes nothing: it never opens a room. An empty
// a.DedupKey turns dedup off for that alert.
func (s *Service) OpenAlertRoom(ctx context.Context, a alertin.Alert) (RoomSnapshot, bool, error) {
	src := string(a.Source)
	if src == "" {
		// An empty source would read back as a pre-source room after a restart.
		src = "alert"
	}
	return s.openAlertRoom(ctx, a, src)
}

// openAlertRoom is OpenAlertRoom with the route recorded in room.created.
func (s *Service) openAlertRoom(ctx context.Context, a alertin.Alert, alertSource string) (RoomSnapshot, bool, error) {
	title, severity, pipelineID, raw := a.Title, a.Severity, a.PipelineID, a.Raw
	// Before ANY write (a repeat joining an open room writes too): refuse
	// what Postgres jsonb/text cannot store, with a 400 instead of a failed
	// append on real Postgres.
	if err := checkJSONBStorable(raw); err != nil {
		return RoomSnapshot{}, false, err
	}
	if err := rejectUnstorable(title); err != nil {
		return RoomSnapshot{}, false, err
	}
	if title == "" {
		title = "New alert"
	}
	title = clipTitle(title)
	if !severities[severity] {
		severity = "sev3"
	}
	if pipelineID == "" {
		pipelineID = "unknown"
	}
	if !pipelineIDRe.MatchString(pipelineID) {
		return RoomSnapshot{}, false, validation(fmt.Sprintf("pipeline_id %q must match ^[a-zA-Z0-9_.-]+$", pipelineID))
	}
	pipelineKnown := s.owners == nil || s.knownPipeline(pipelineID)
	// Frozen in room.created so a restart keeps it even if owners.yaml
	// changes (docs/design/shadow.md 3.1). Only a known pipeline can be
	// shadow.
	shadow := s.pipelineShadow(pipelineID)
	resolved := a.Status == alertin.StatusResolved
	indexKey := alertIndexKey(pipelineID, a.DedupKey)
	roomID := NewID("r_")
	now := s.now().UTC()

	// The same alert may arrive twice at once, here or, with a shared store,
	// on two replicas: the key lock (taken before any room lock, never under
	// one) makes them open one room.
	unlock, err := s.lockAlertKey(ctx, indexKey)
	if err != nil {
		return RoomSnapshot{}, false, err
	}
	defer unlock()
	// Shared store: bring the key's room up to the log (or load the one
	// another replica opened) before any room lock is taken.
	shared := s.prepareAlertKeyRoom(ctx, indexKey)

	// The same alert while its room is still open joins that room: no second
	// room, no second agent, and the hook is not called again (the room was
	// opened before).
	existing, ok := s.openAlertRoomLocked(indexKey)
	if !ok {
		existing, ok = s.openAlertRoomSharedLocked(indexKey, shared)
	}
	if ok {
		if st := s.lockCachedRoom(existing); st != nil {
			if s.roomOpen(st) {
				snap, err := s.joinAlertRoomLocked(ctx, existing, st, title, raw, resolved)
				s.unlockRoom(st)
				return snap, false, err
			}
			// Closed between the look-up and the lock (a close does not take
			// the key lock): the alert belongs to a new room.
			s.unlockRoom(st)
		}
	}
	if resolved {
		return RoomSnapshot{}, false, ErrResolvedAlertNoRoom
	}
	if err := s.store.CreateRoom(ctx, store.Room{
		ID: roomID, Title: title, Severity: severity, PipelineID: pipelineID, Status: "open", OpenedAt: now,
	}); err != nil {
		return RoomSnapshot{}, false, fmt.Errorf("room: create from alert: %w", err)
	}
	st := &roomState{snap: NewSnapshot(), inbox: newRoomInbox(shadow), openingFor: pipelineID}
	st.snap.ID = roomID
	s.lockState(st)
	s.cacheRoom(roomID, st)

	created, err := s.emitLocked(ctx, st, roomID, events.RoomCreated, systemActor, events.RoomCreatedPayload{
		Title: title, Severity: severity, PipelineID: pipelineID, Alert: raw,
		AlertSource: alertSource, DedupKey: indexKey, Shadow: shadow,
	})
	if err != nil {
		s.discardNewRoomLocked(ctx, roomID, st)
		s.unlockRoom(st)
		return RoomSnapshot{}, false, mapUnstorable(err)
	}
	// Later runs of the agent start from the alert as the log holds it, as
	// a room loaded from the log does.
	if p, ok := decode[events.RoomCreatedPayload](created); ok {
		st.alertRaw = p.Alert
	}
	if shadow {
		// First try; the supervisor and the Load sweep try again, and a
		// failure never drops the alert (docs/design/shadow.md 3.7).
		_ = s.createShadowRun(ctx, roomID)
	}
	if indexKey != "" {
		s.setAlertKey(roomID, st, indexKey, true)
	}
	// A missing/unknown pipeline opens with a system message so the room shows
	// why no fix is proposed. events.go has no dedicated system-message type,
	// so the message is an agent.message carried by the system actor (the room
	// renders it with the actor name "System").
	if !pipelineKnown {
		if s.TenancyOn() {
			// No tenant holds this room, so nobody sees it in the UI; the
			// operator has to fix the alert's pipeline.
			s.log.Warn("room: alert room for a pipeline owners.yaml does not define is visible to no tenant",
				"room", roomID, "pipeline", pipelineID, "alert_source", alertSource)
		}
		// Only a note: proposals stay disabled without it (proposalsDisabled
		// reads the pipeline id), and the room is already open (afterAppend).
		_, err := s.emitLocked(ctx, st, roomID, events.AgentMessage, systemActor, events.AgentMessagePayload{
			Text: fmt.Sprintf("pipeline %q is unknown; proposals are disabled for this room", pipelineID),
		})
		s.afterAppend(roomID, "unknown pipeline note", err, "pipeline", pipelineID)
	}
	snap := st.snap.Clone()
	st.firstLook = true
	s.startAgentLocked(roomID, title, pipelineID, raw, st)
	s.notifyRoomOpenedLocked(roomID)
	s.unlockRoom(st)
	return snap, true, nil
}

// alertDedup reads a webhook body the way alertin does and returns the dedup
// key that makes a repeat of the same alert join the room already open for it
// (alertin.Alert.DedupKey), and whether the source reports the alert resolved.
// The key is derived from the raw body alone, so a restart rebuilds it. Without
// a dedup_key in the body the key is a hash of the body: only byte-identical
// bodies are deduplicated. A body alertin cannot parse, or no body, gets no key
// and always opens a room.
func alertDedup(raw json.RawMessage) (key string, resolved bool) {
	if len(raw) == 0 {
		return "", false
	}
	a, err := alertin.Config{}.Parse(alertin.Generic, raw)
	if err != nil {
		return "", false
	}
	return a.DedupKey, a.Status == alertin.StatusResolved
}

// alertIndexKey scopes an alert dedup key to its pipeline: the same key sent for
// two pipelines is two incidents. An empty dedup key stays empty (no dedup).
// The key is stored in room.created (Postgres jsonb), so the separator must be
// printable: jsonb rejects \u0000. "|" cannot occur in a pipeline id
// ([a-zA-Z0-9_.-]), so pipeline and key never run together.
func alertIndexKey(pipelineID, dedupKey string) string {
	if dedupKey == "" {
		return ""
	}
	return pipelineID + "|" + dedupKey
}

// openAlertRoomLocked returns the open room that already holds indexKey (see
// alertIndexKey), if any. A key whose room is closed is not a match. With a
// shared store the caller has brought the room up to the log first
// (prepareAlertKeyRoom), so a close on another replica counts. It judges the
// room by what it published; the caller locks it and checks again. Caller
// holds the alert key lock (lockAlertKey).
func (s *Service) openAlertRoomLocked(indexKey string) (string, bool) {
	if indexKey == "" {
		return "", false
	}
	roomID, st := s.alertRoomFor(indexKey)
	if st == nil {
		return "", false
	}
	if p := st.pub.Load(); p == nil || p.Status != "open" {
		return "", false
	}
	return roomID, true
}

// joinAlertRoomLocked records a repeat of an alert in the room that is already
// open for it, as a system message, and returns that room. A repeat that the
// source reports resolved does not close the room: only the driver does that.
// A firing repeat also reaches the agent as a quiet alert update with raw's
// evidence (alertUpdateLocked). Caller holds st's lock.
func (s *Service) joinAlertRoomLocked(ctx context.Context, roomID string, st *roomState, title string, raw json.RawMessage, resolved bool) (RoomSnapshot, error) {
	text := fmt.Sprintf("alert %q fired again; joined this room instead of opening a new one", title)
	if resolved {
		text = fmt.Sprintf("alert %q reported resolved at its source; the room stays open until the driver closes it", title)
	}
	e, err := s.emitLocked(ctx, st, roomID, events.AgentMessage, systemActor, events.AgentMessagePayload{Text: text})
	if err != nil {
		// A full log cannot take the note, but the alert still belongs to
		// this room: answering an error would make the sender retry forever.
		if !errors.Is(err, store.ErrLogFull) {
			return RoomSnapshot{}, err
		}
		s.log.Warn("room: repeat alert joined a room whose log is full; note not stored", "room", roomID)
	}
	if !resolved {
		s.alertUpdateLocked(roomID, st, e.Seq, raw)
	}
	return st.snap.Clone(), nil
}

// alertIndexKeyOf returns the index key a room was opened under, from its
// room.created payload p and its pipeline. The key is read back exactly as
// stored in room.created (DedupKey); it is never re-derived from the body, so
// a dev-webhook room cannot end up under a signed source's key. A room from
// before the source was recorded (AlertSource empty) can only have come from
// the dev webhook, so it gets the webhook key. "" for a room no alert opened.
func alertIndexKeyOf(p events.RoomCreatedPayload, pipelineID string) string {
	if p.AlertSource != "" {
		return p.DedupKey
	}
	dedupKey, _ := alertDedup(p.Alert)
	if dedupKey == "" {
		return ""
	}
	return alertIndexKey(pipelineID, webhookKeyPrefix+dedupKey)
}

// releaseAlertKeyLocked drops a closed room's alert key and hands it to another
// open room that holds the same key, if there is one (only possible for rooms
// opened before dedup existed). The other rooms are judged by what they last
// published, so no other room's lock is taken. Caller holds st's lock.
func (s *Service) releaseAlertKeyLocked(roomID string, st *roomState) {
	s.reg.Lock()
	defer s.reg.Unlock()
	key := st.dedupKey
	if key == "" || s.alertRooms[key] != roomID {
		return
	}
	delete(s.alertRooms, key)
	for id, other := range s.rooms {
		if id == roomID || other.dedupKey != key {
			continue
		}
		if p := other.pub.Load(); p != nil && p.Status == "open" {
			s.alertRooms[key] = id
			return
		}
	}
}

// notifyRoomOpenedLocked runs the OnRoomOpened hook off the request path, so
// a slow hook never delays the HTTP response. After Close the hook is not
// called (addWorker).
func (s *Service) notifyRoomOpenedLocked(roomID string) {
	if s.onRoomOpened == nil || !s.addWorker() {
		return
	}
	go func() {
		defer s.wg.Done()
		s.onRoomOpened(s.ctx, roomID)
	}()
}

// GetRoom returns a room snapshot or a not-found error. A cached room answers
// from its published snapshot, without waiting for a command in flight.
func (s *Service) GetRoom(ctx context.Context, roomID string) (RoomSnapshot, error) {
	p, err := s.readRoom(ctx, roomID)
	if err != nil {
		return RoomSnapshot{}, err
	}
	return p.Clone(), nil
}

// ListRooms returns every room, open rooms first, as each was last published.
func (s *Service) ListRooms(ctx context.Context) ([]RoomSnapshot, error) {
	// The store is read outside every lock; a room lock is taken only for a
	// room that has to be loaded or folded (refreshAll).
	if err := s.refreshAll(ctx); err != nil {
		return nil, err
	}
	rooms := s.publishedRooms()
	out := make([]RoomSnapshot, 0, len(rooms))
	for _, p := range rooms {
		out = append(out, p.Clone())
	}
	sort.SliceStable(out, func(i, j int) bool {
		if (out[i].Status == "open") != (out[j].Status == "open") {
			return out[i].Status == "open"
		}
		return out[i].OpenedAt.After(out[j].OpenedAt)
	})
	return out, nil
}

// Events returns events after afterSeq and the room's current last_seq.
func (s *Service) Events(ctx context.Context, roomID string, afterSeq int64, limit int) ([]events.Event, int64, error) {
	if _, err := s.readRoom(ctx, roomID); err != nil {
		return nil, 0, err
	}
	if limit <= 0 {
		limit = 500
	}
	if limit > 1000 {
		limit = 1000
	}
	// One event more than asked: when it exists the log goes on past this
	// page, and last_seq says so even while the published snapshot trails
	// an append still in flight, so a client paging up to last_seq does not
	// stop early.
	evs, err := s.store.Since(ctx, roomID, afterSeq, limit+1)
	if err != nil {
		return nil, 0, fmt.Errorf("room: events: %w", err)
	}
	lastSeq := int64(0)
	if p := s.published(roomID); p != nil {
		lastSeq = p.LastSeq
	} else if row, err := s.store.GetRoom(ctx, roomID); err == nil {
		lastSeq = row.LastSeq
	}
	if len(evs) > limit {
		lastSeq = max(lastSeq, evs[limit].Seq)
		evs = evs[:limit]
	}
	// Never below an event this answer holds.
	if n := len(evs); n > 0 {
		lastSeq = max(lastSeq, evs[n-1].Seq)
	}
	return evs, lastSeq, nil
}

// CloseRoom closes the room (driver only) and stops the agent.
func (s *Service) CloseRoom(ctx context.Context, user User, roomID, resolution string) (events.Event, error) {
	if err := rejectUnstorable(resolution); err != nil {
		return events.Event{}, err
	}
	return s.closeRoom(ctx, roomID, s.humanActor(user), func(st *roomState) (string, error) {
		if !s.isDriver(st, user.ID) {
			return "", ErrNotDriver()
		}
		// The resolution is what the shadow run is graded against.
		if st.snap.Shadow && strings.TrimSpace(resolution) == "" {
			return "", validation("a shadow room needs a resolution to grade against")
		}
		return resolution, nil
	})
}

// repairClosedRow closes the rooms row of a room the log says is closed when
// the row still says open: the store.CloseRoom after room.closed failed
// (afterAppend), and nothing else writes it. Retention picks closed rooms from
// the rows, and lists and counts read them, so the row must follow the log.
// The closed time is room.closed's own, the same on every replica, so any
// replica may repair it and a repeat writes the same row (CloseRoom is a
// plain UPDATE). Best effort: a failure is logged and the next load or
// retention pass tries again.
func (s *Service) repairClosedRow(ctx context.Context, row store.Room, snap *RoomSnapshot) {
	if row.Status != "open" || snap.Status != "closed" || snap.ClosedAt == nil {
		return
	}
	if err := s.store.CloseRoom(ctx, row.ID, snap.ClosedAt.UTC()); err != nil {
		s.log.Warn("room: repair of a closed room's row failed", "room", row.ID, "err", err)
		return
	}
	s.log.Info("room: repaired a closed room's row from the log", "room", row.ID)
}

// closeRoom closes an open room as actor. check runs under the room's lock
// after the open check and returns the resolution to record, or an error that
// stops the close. The driver's close and the open-room limit's close share
// everything else: the alert key release, the persisted close, the shadow
// grace and reveal, the agent and executor stop and the room-closed hook.
//
// With a shared store the room is first brought up to the log (reading outside
// the lock), and the close is a checked append: a close and a control change on
// two replicas at once cannot both pass (docs/design/replicas.md 9.1).
func (s *Service) closeRoom(ctx context.Context, roomID string, actor events.Actor, check func(st *roomState) (string, error)) (events.Event, error) {
	s.syncRoom(ctx, roomID)
	st, err := s.lockRoom(ctx, roomID)
	if err != nil {
		return events.Event{}, err
	}
	if !s.roomOpen(st) {
		s.unlockRoom(st)
		return events.Event{}, roomClosed("room already closed")
	}
	resolution, err := check(st)
	if err != nil {
		s.unlockRoom(st)
		return events.Event{}, err
	}
	e, err := s.emitCheckedLocked(ctx, st, roomID, func() error {
		if !s.roomOpen(st) {
			return roomClosed("room already closed")
		}
		_, cerr := check(st) // same resolution; the rule must still hold
		return cerr
	}, events.RoomClosed, actor, events.RoomClosedPayload{Resolution: resolution})
	if err != nil {
		s.unlockRoom(st)
		return events.Event{}, err
	}
	// A closed room no longer takes repeats of its alert: the next one opens a
	// new room.
	s.releaseAlertKeyLocked(roomID, st)
	s.afterAppend(roomID, "persist close", s.store.CloseRoom(ctx, roomID, s.now().UTC()))
	cancel := st.agentCancel
	if st.snap.Shadow {
		// Grace, the error-row upsert and the reveal (shadow.go). It keeps
		// cancel only when the run must stop now.
		cancel = s.closeShadowLocked(ctx, roomID, st, cancel)
	} else {
		// A closed room needs no agent owner. (A shadow room keeps its lease
		// until its run has finished, grace included: finishShadow.)
		s.dropLeaseLocked(roomID, st)
	}
	execCancel := st.execCancel
	pipelineID := st.snap.PipelineID
	hookKey := hookDedupKey(pipelineID, s.dedupKeyOf(st))
	runHook := s.onRoomClosed != nil && !s.isClosed()
	// A shadow room's log has no agent events: no draft. The drafter's place
	// in wg is taken here (addWorker), so Close's Wait cannot miss it.
	runDraft := !st.snap.Shadow && s.addWorker()
	s.unlockRoom(st)
	if cancel != nil {
		cancel()
	}
	if execCancel != nil {
		execCancel()
	}
	// Outside the room's lock, so the hook may call OpenRoomForPipeline and the
	// like.
	if runHook {
		s.onRoomClosed(s.ctx, roomID, pipelineID, hookKey)
	}
	if runDraft {
		go s.draftPostmortem(roomID, true)
	}
	return e, nil
}
