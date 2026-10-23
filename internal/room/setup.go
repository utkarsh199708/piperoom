package room

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/store"
)

// Setup rooms (U wave; docs/design/setup-agent.md, docs/api.md "Setup rooms").
// POST /api/setup opens one from a SetupRequest; the driver's clicks grow its
// target list, its provisional pipeline and its maintenance window. Every
// command is driver only, on an open setup room, and writes exactly one
// setup.* event: the log is the record, and the snapshot folds from it.

// SetupRoom is the result of POST /api/setup: the new room's view and the
// single-use pairing code bound to (tenant, room, creator).
type SetupRoom struct {
	Room      RoomSnapshot
	Code      string
	ExpiresAt time.Time
}

// setupNameRe rules for the provisional pipeline's lists. Tables are
// schema.table (the shape owners.PlainTable names); a connector is a Connect
// connector name; a consumer group is a Kafka group id; a topic prefix is the
// prefix of every topic the pipeline owns.
var (
	setupTableRe         = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$`)
	setupConnectorRe     = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]*$`)
	setupConsumerGroupRe = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.:-]*$`)
	setupTopicPrefixRe   = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)
	setupRepoIDRe        = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
)

// setupSourceKinds and setupRequestTargets are the values
// events.SetupRequest.SourceKind and .Target document.
var (
	setupSourceKinds = map[string]bool{
		"postgres": true, "mysql": true, "sqlserver": true, "mongodb": true, "oracle": true,
	}
	setupRequestTargets = map[string]bool{
		"compose": true, "host": true, "strimzi": true, "msk": true,
		"confluent": true, "aiven": true, "dbzserver": true, "embedded": true,
	}
)

// CreateSetupRoom opens a setup room (POST /api/setup): a room whose alert is
// the request, whose creator is the driver, and whose pipeline is the
// provisional one owners.Provisional builds from the request. It issues the
// single-use pairing code bound to (tenant, room, creator) and leaves the
// agent unstarted: a setup run begins when the setup mode lands (U1-10), so
// this room never investigates an incident that does not exist.
func (s *Service) CreateSetupRoom(ctx context.Context, user User, req events.SetupRequest) (SetupRoom, error) {
	if !s.setupCfg.Enabled {
		return SetupRoom{}, setupDisabled()
	}
	title := stripBidi(strings.TrimSpace(req.Title))
	if err := rejectUnstorable(title); err != nil {
		return SetupRoom{}, err
	}
	if title == "" {
		return SetupRoom{}, validation("title is required")
	}
	if err := checkLen("title", title, MaxTitleChars); err != nil {
		return SetupRoom{}, err
	}
	// Tenancy (design §3.6): with a tenants: block the request names one and
	// its creator must be a member; without one the tenant must be absent.
	switch {
	case s.TenancyOn():
		tenant := strings.TrimSpace(req.Tenant)
		if tenant == "" {
			return SetupRoom{}, validation("tenant is required on this server")
		}
		if !s.owners.InTenant(tenant, user.ID, user.Groups) {
			return SetupRoom{}, notTenantMember()
		}
		req.Tenant = tenant
	case strings.TrimSpace(req.Tenant) != "":
		return SetupRoom{}, validation("tenant must be empty: this server has no tenants: block")
	}

	req.PipelineID = strings.TrimSpace(req.PipelineID)
	if !pipelineIDRe.MatchString(req.PipelineID) {
		return SetupRoom{}, validation(fmt.Sprintf("pipeline_id %q must match ^[a-zA-Z0-9_.-]+$", req.PipelineID))
	}
	// The id names <PIPEROOM_SECRETS_DIR>/<pipeline> later: "." and ".." are
	// path syntax, not names ("." would open the secrets root itself).
	if !secretsPipelineID(req.PipelineID) {
		return SetupRoom{}, validation(fmt.Sprintf("pipeline_id %q must contain a character other than '.'", req.PipelineID))
	}
	if s.owners.HasPipeline(req.PipelineID) {
		return SetupRoom{}, pipelineExists(req.PipelineID)
	}
	if s.openSetupRoomFor(req.PipelineID) {
		// A second setup of the same pipeline would scope two rooms to one
		// provisional pipeline and register it twice.
		return SetupRoom{}, pipelineExists(req.PipelineID)
	}
	req.Title = title
	if err := validateSetupRequest(&req); err != nil {
		return SetupRoom{}, err
	}
	targets, plans, err := s.planSetupTargets(ctx, req.Targets)
	if err != nil {
		return SetupRoom{}, err
	}
	req.Targets = targets
	if _, err := owners.Provisional(req); err != nil {
		return SetupRoom{}, validation("the setup request does not describe a pipeline: " + err.Error())
	}
	raw, err := json.Marshal(req)
	if err != nil {
		return SetupRoom{}, fmt.Errorf("room: marshal setup request: %w", err)
	}
	if err := checkJSONBStorable(raw); err != nil {
		return SetupRoom{}, err
	}

	// Two concurrent creates of the same pipeline must not both pass the
	// pipeline checks: the key lock (the alert webhook's striped lock, and
	// with a shared store the cluster-wide one) makes the re-check and the
	// create one step, and it is held until the room is published, so the
	// loser's re-check sees the winner's open room. The key starts with
	// "setup ", and a space cannot occur in a pipeline id, so it never
	// collides with an alert's pipeline|dedup index key. The unlocked checks
	// above remain the fast fail before any target resolution.
	unlock, err := s.lockAlertKey(ctx, "setup "+req.PipelineID)
	if err != nil {
		return SetupRoom{}, err
	}
	defer unlock()
	if s.owners.HasPipeline(req.PipelineID) || s.openSetupRoomFor(req.PipelineID) {
		return SetupRoom{}, pipelineExists(req.PipelineID)
	}

	roomID := NewID("r_")
	now := s.now().UTC()
	if err := s.store.CreateRoom(ctx, store.Room{
		ID: roomID, Title: title, Severity: setupSeverity, PipelineID: req.PipelineID, Status: "open", OpenedAt: now,
	}); err != nil {
		return SetupRoom{}, fmt.Errorf("room: create setup room: %w", err)
	}
	st := &roomState{snap: NewSnapshot(), inbox: newInbox(), openingFor: req.PipelineID}
	st.snap.ID = roomID
	s.lockState(st)
	s.cacheRoom(roomID, st)

	creator := s.humanActor(user)
	code, err := s.runners.IssuePairingCode(req.Tenant, roomID, creator)
	if err != nil {
		// No event was written yet, so the room can go again with nothing left
		// behind. The caller retries and gets a fresh code.
		s.discardNewRoomLocked(ctx, roomID, st)
		s.unlockRoom(st)
		return SetupRoom{}, fmt.Errorf("room: create setup room: %w", err)
	}

	created, err := s.emitLocked(ctx, st, roomID, events.RoomCreated, creator, events.RoomCreatedPayload{
		Title: title, Severity: setupSeverity, PipelineID: req.PipelineID, Alert: raw,
		Mode: events.RoomModeSetup,
	})
	if err != nil {
		s.discardNewRoomLocked(ctx, roomID, st)
		s.unlockRoom(st)
		return SetupRoom{}, mapUnstorable(err)
	}
	// Later runs of the agent start from the request as the log holds it.
	if p, ok := decode[events.RoomCreatedPayload](created); ok {
		st.alertRaw = p.Alert
	}
	// Every class decision is audited, including the request's floors
	// (docs/api.md: setup.target_classed records every one).
	for _, t := range plans {
		actor := systemActor
		if t.by == "person" {
			actor = creator
		}
		if _, err := s.emitLocked(ctx, st, roomID, events.SetupTargetClassed, actor, events.SetupTargetClassedPayload{
			Target: t.target.Address, Class: t.class, Reason: t.reason, By: t.by,
		}); err != nil {
			// The room exists (room.created landed): never fail the create for
			// an audit event that follows it.
			s.afterAppend(roomID, "record the setup request's target classes", err, "target", t.target.Address)
			break
		}
	}
	// The creator drives the new room, as POST /api/rooms does.
	if _, err := s.emitLocked(ctx, st, roomID, events.ControlGranted, creator, events.ControlGrantedPayload{
		To: creator, LeaseUntil: now.Add(DriverLease),
	}); err != nil {
		s.afterAppend(roomID, "grant control to the setup room's creator", err, "user", user.ID)
	} else {
		s.afterAppend(roomID, "persist creator driver", s.store.UpdateDriver(ctx, roomID, user.ID, user.Name, now.Add(DriverLease)))
	}
	snap := st.snap.Clone()
	s.notifyRoomOpenedLocked(roomID)
	s.unlockRoom(st)
	return SetupRoom{Room: snap, Code: code, ExpiresAt: now.Add(protocol.CodeTTL)}, nil
}

// planSetupTargets validates every target of the request and combines its
// floor with the derived class.
func (s *Service) planSetupTargets(ctx context.Context, in []events.SetupTarget) ([]events.SetupTarget, []setupTargetPlan, error) {
	// The cap comes before any plan: each target can cost a name lookup, so
	// an oversized list must not drive the resolver at all.
	if len(in) > maxSetupTargets {
		return nil, nil, validation(fmt.Sprintf("targets: at most %d entries", maxSetupTargets))
	}
	out := make([]events.SetupTarget, 0, len(in))
	plans := make([]setupTargetPlan, 0, len(in))
	seen := map[string]bool{}
	for _, t := range in {
		key := strings.TrimSpace(t.Address)
		if seen[key] {
			return nil, nil, validation("the setup request lists the same target address twice")
		}
		plan, err := s.planSetupTarget(ctx, t)
		if err != nil {
			return nil, nil, err
		}
		seen[key] = true
		out = append(out, plan.target)
		plans = append(plans, plan)
	}
	return out, plans, nil
}

// openSetupRoomFor reports whether an open setup room already owns pipelineID.
func (s *Service) openSetupRoomFor(pipelineID string) bool {
	for _, snap := range s.publishedRooms() {
		if snap.Status == "open" && snap.PipelineID == pipelineID && snap.IsSetupRoom() {
			return true
		}
	}
	return false
}

// validateSetupRequest checks the request's typed fields: the kinds, the topic
// prefix and every name of the provisional pipeline. It changes nothing.
func validateSetupRequest(req *events.SetupRequest) error {
	source := strings.TrimSpace(req.SourceKind)
	if !setupSourceKinds[source] {
		return validation(fmt.Sprintf("source_kind %q: want one of postgres, mysql, sqlserver, mongodb, oracle", req.SourceKind))
	}
	req.SourceKind = source
	target := strings.TrimSpace(req.Target)
	if !setupRequestTargets[target] {
		return validation(fmt.Sprintf("target %q: want one of compose, host, strimzi, msk, confluent, aiven, dbzserver, embedded", req.Target))
	}
	req.Target = target
	prefix := strings.TrimSpace(req.TopicPrefix)
	if prefix == "" {
		return validation("topic_prefix is required")
	}
	if !setupTopicPrefixRe.MatchString(prefix) || len(prefix) > maxSetupNameChars {
		return validation("topic_prefix must start with a letter or digit and hold letters, digits, '.', '_' and '-'")
	}
	req.TopicPrefix = prefix
	if len(req.Tables) == 0 {
		return validation("tables is required: name at least one source table (schema.table)")
	}
	var err error
	if req.Tables, err = cleanSetupNames("tables", req.Tables, setupTableRe, "a schema.table name"); err != nil {
		return err
	}
	if req.Connectors, err = cleanSetupNames("connectors", req.Connectors, setupConnectorRe, "a connector name"); err != nil {
		return err
	}
	if req.Repos, err = cleanSetupNames("repos", req.Repos, setupRepoIDRe, "a repo id"); err != nil {
		return err
	}
	return nil
}

// cleanSetupNames trims one list and checks every entry: not empty, the list's
// shape, not repeated, and within the cap. It returns the trimmed list (nil for
// an absent one) and leaves the input alone.
func cleanSetupNames(field string, in []string, re *regexp.Regexp, what string) ([]string, error) {
	if in == nil {
		return nil, nil
	}
	if len(in) > maxSetupPipelineList {
		return nil, validation(fmt.Sprintf("%s: at most %d entries", field, maxSetupPipelineList))
	}
	out := make([]string, 0, len(in))
	seen := map[string]bool{}
	for i, raw := range in {
		name := strings.TrimSpace(raw)
		switch {
		case name == "":
			return nil, validation(fmt.Sprintf("%s[%d]: empty entry", field, i))
		case len(name) > maxSetupNameChars:
			return nil, validation(fmt.Sprintf("%s: an entry is longer than %d characters", field, maxSetupNameChars))
		case hasControlChars(name):
			return nil, validation("text contains characters that cannot be stored")
		case !re.MatchString(name):
			return nil, validation(fmt.Sprintf("%s: %q is not %s", field, name, what))
		case seen[name]:
			return nil, validation(fmt.Sprintf("%s lists %q twice", field, name))
		}
		seen[name] = true
		out = append(out, name)
	}
	return out, nil
}

// lockSetupRoom takes the room's lock and applies the rules every setup route
// shares: the room exists in the caller's tenants (404 otherwise), setup is on
// (404 setup_disabled), it is a setup room (400 not_setup_room), it is open
// (409 room_closed) and the caller drives it (403 not_driver). The caller
// unlocks the returned state.
func (s *Service) lockSetupRoom(ctx context.Context, user User, roomID string) (*roomState, error) {
	st, err := s.lockRoom(ctx, roomID)
	if err != nil {
		return nil, err
	}
	if s.TenancyOn() && !s.canSeeRoom(user, &st.snap) {
		s.unlockRoom(st)
		return nil, errHidden()
	}
	if !s.setupCfg.Enabled {
		s.unlockRoom(st)
		return nil, setupDisabled()
	}
	if !st.snap.IsSetupRoom() {
		s.unlockRoom(st)
		return nil, notSetupRoom()
	}
	if !s.roomOpen(st) {
		s.unlockRoom(st)
		return nil, roomClosed("room is closed")
	}
	if !s.isDriver(st, user.ID) {
		s.unlockRoom(st)
		return nil, ErrNotDriver()
	}
	return st, nil
}

// AddSetupTarget is POST /api/rooms/{id}/setup/targets: the driver adds one
// host the ops may reach. The address goes through the denylist of design
// §2.6, the class is max(floor, derived) and never below shared, and the room
// emits setup.target_registered plus setup.target_classed.
func (s *Service) AddSetupTarget(ctx context.Context, user User, roomID string, in events.SetupTarget) (events.Event, error) {
	// The caller's right to touch the room is settled first: planSetupTarget
	// resolves names (up to targetResolveTimeout per host) and answers address
	// verdicts, and neither that resolver work nor a verdict may be observable
	// by a caller the room would refuse (403 not_driver, 404). The lock is
	// given back for the resolution, because no slow call runs under a room
	// lock, and taken again below.
	st, err := s.lockSetupRoom(ctx, user, roomID)
	if err != nil {
		return events.Event{}, err
	}
	s.unlockRoom(st)
	plan, err := s.planSetupTarget(ctx, in)
	if err != nil {
		return events.Event{}, err
	}
	// Re-locked and re-verified: the room may have closed or changed drivers
	// while the names resolved.
	st, err = s.lockSetupRoom(ctx, user, roomID)
	if err != nil {
		return events.Event{}, err
	}
	defer s.unlockRoom(st)
	if len(st.snap.Setup.Targets) >= maxSetupTargets {
		return events.Event{}, validation(fmt.Sprintf("the room already has %d targets; at most %d", len(st.snap.Setup.Targets), maxSetupTargets))
	}
	for _, t := range st.snap.Setup.Targets {
		if t.Address == plan.target.Address {
			return events.Event{}, validation("that target address is already registered")
		}
	}
	actor := s.humanActor(user)
	e, err := s.emitLocked(ctx, st, roomID, events.SetupTargetRegistered, actor, events.SetupTargetRegisteredPayload{
		Kind: plan.target.Kind, Address: plan.target.Address, Class: plan.class, By: actor,
	})
	if err != nil {
		return events.Event{}, err
	}
	if _, err := s.emitLocked(ctx, st, roomID, events.SetupTargetClassed, actor, events.SetupTargetClassedPayload{
		Target: plan.target.Address, Class: plan.class, Reason: plan.reason, By: plan.by,
	}); err != nil {
		s.afterAppend(roomID, "record the new target's class", err, "target", plan.target.Address)
	}
	s.touchDriverLease(ctx, st, roomID)
	return e, nil
}

// RaiseSetupTargetClass is POST /api/rooms/{id}/setup/targets/{t}/class: the
// driver raises one target's class. A class only rises, so a lower or equal
// class is 400 class_lowered. t is the address as registered.
func (s *Service) RaiseSetupTargetClass(ctx context.Context, user User, roomID, address, class string) (events.Event, error) {
	class = strings.TrimSpace(class)
	if !validSetupClass(class) {
		return events.Event{}, validation(fmt.Sprintf("class %q: want local, shared or production", class))
	}
	st, err := s.lockSetupRoom(ctx, user, roomID)
	if err != nil {
		return events.Event{}, err
	}
	defer s.unlockRoom(st)
	address = strings.TrimSpace(address)
	found := false
	current := ""
	for _, t := range st.snap.Setup.Targets {
		if t.Address == address {
			found, current = true, t.Class
			break
		}
	}
	if !found {
		return events.Event{}, notFound("no such target")
	}
	if setupClassRank[class] <= setupClassRank[current] {
		return events.Event{}, classLowered(fmt.Sprintf("the target's class is %q; a class can only be raised", current))
	}
	actor := s.humanActor(user)
	e, err := s.emitLocked(ctx, st, roomID, events.SetupTargetClassed, actor, events.SetupTargetClassedPayload{
		Target: address, Class: class, Reason: "person", By: "person",
	})
	if err != nil {
		return events.Event{}, err
	}
	s.touchDriverLease(ctx, st, roomID)
	return e, nil
}

// SetupPipelineExtension is the body of POST /api/rooms/{id}/setup/pipeline.
type SetupPipelineExtension struct {
	AddConnectors     []string
	AddTables         []string
	AddConsumerGroups []string
	AddSinkTables     []string
}

// empty reports whether the extension adds nothing.
func (x SetupPipelineExtension) empty() bool {
	return len(x.AddConnectors) == 0 && len(x.AddTables) == 0 && len(x.AddConsumerGroups) == 0 && len(x.AddSinkTables) == 0
}

// ExtendSetupPipeline is POST /api/rooms/{id}/setup/pipeline: the driver grows
// the provisional pipeline, which changes the scope the setup tools and
// actions check. A name the pipeline already holds is refused, so the event
// always says what is new.
func (s *Service) ExtendSetupPipeline(ctx context.Context, user User, roomID string, in SetupPipelineExtension) (events.Event, error) {
	if in.empty() {
		return events.Event{}, validation("nothing to add: name at least one connector, table, consumer group or sink table")
	}
	var err error
	if in.AddConnectors, err = cleanSetupNames("add_connectors", in.AddConnectors, setupConnectorRe, "a connector name"); err != nil {
		return events.Event{}, err
	}
	if in.AddTables, err = cleanSetupNames("add_tables", in.AddTables, setupTableRe, "a schema.table name"); err != nil {
		return events.Event{}, err
	}
	if in.AddConsumerGroups, err = cleanSetupNames("add_consumer_groups", in.AddConsumerGroups, setupConsumerGroupRe, "a consumer group id"); err != nil {
		return events.Event{}, err
	}
	if in.AddSinkTables, err = cleanSetupNames("add_sink_tables", in.AddSinkTables, setupTableRe, "a schema.table name"); err != nil {
		return events.Event{}, err
	}
	st, err := s.lockSetupRoom(ctx, user, roomID)
	if err != nil {
		return events.Event{}, err
	}
	defer s.unlockRoom(st)
	p := st.snap.Setup.Provisional
	for _, l := range []struct {
		field    string
		add      []string
		existing []string
	}{
		{"add_connectors", in.AddConnectors, p.Connectors},
		{"add_tables", in.AddTables, p.Tables},
		{"add_consumer_groups", in.AddConsumerGroups, p.ConsumerGroups},
		{"add_sink_tables", in.AddSinkTables, p.SinkTables},
	} {
		for _, name := range l.add {
			for _, have := range l.existing {
				if name == have {
					return events.Event{}, validation(fmt.Sprintf("%s: %q is already in the pipeline", l.field, name))
				}
			}
		}
	}
	actor := s.humanActor(user)
	e, err := s.emitLocked(ctx, st, roomID, events.SetupPipelineExtended, actor, events.SetupPipelineExtendedPayload{
		AddConnectors:     in.AddConnectors,
		AddTables:         in.AddTables,
		AddConsumerGroups: in.AddConsumerGroups,
		AddSinkTables:     in.AddSinkTables,
		By:                actor,
	})
	if err != nil {
		return events.Event{}, err
	}
	s.touchDriverLease(ctx, st, roomID)
	return e, nil
}

// RecordMaintenanceWindow is POST /api/rooms/{id}/setup/maintenance-window:
// the driver records when a T3 step may run. At most 4 hours long, started at
// most 24 hours ahead, ending in the future (design §3.5, docs/api.md).
func (s *Service) RecordMaintenanceWindow(ctx context.Context, user User, roomID string, start, end time.Time) (events.Event, error) {
	now := s.now().UTC()
	start, end = start.UTC(), end.UTC()
	if !end.After(start) {
		return events.Event{}, validation("the window's end must be after its start")
	}
	if !end.After(now) {
		return events.Event{}, validation("the window has already ended")
	}
	if end.Sub(start) > MaxSetupWindowDuration {
		return events.Event{}, windowTooLong(fmt.Sprintf("a maintenance window is at most %s long", MaxSetupWindowDuration))
	}
	if start.After(now.Add(MaxSetupWindowAhead)) {
		return events.Event{}, windowTooLong(fmt.Sprintf("a maintenance window starts at most %s ahead", MaxSetupWindowAhead))
	}
	st, err := s.lockSetupRoom(ctx, user, roomID)
	if err != nil {
		return events.Event{}, err
	}
	defer s.unlockRoom(st)
	actor := s.humanActor(user)
	e, err := s.emitLocked(ctx, st, roomID, events.SetupMaintenanceWindow, actor, events.SetupMaintenanceWindowPayload{
		Start: start, End: end, By: actor,
	})
	if err != nil {
		return events.Event{}, err
	}
	s.touchDriverLease(ctx, st, roomID)
	return e, nil
}
