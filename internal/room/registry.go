package room

// The room registry: which rooms are cached, the alert index, and whether the
// service is closed. s.reg guards it. reg is a leaf lock: no I/O, no hook and
// no other lock of the service is taken while it is held, so any code may
// take it, whatever else it holds. The helpers below are the only code that
// takes it; none of them calls back into the service.

// cached returns the cached state of roomID, or nil.
func (s *Service) cached(roomID string) *roomState {
	s.reg.Lock()
	defer s.reg.Unlock()
	return s.rooms[roomID]
}

// cachedRooms returns every cached room by id. It is a copy: the caller may
// range over it while rooms enter and leave the cache.
func (s *Service) cachedRooms() map[string]*roomState {
	s.reg.Lock()
	defer s.reg.Unlock()
	out := make(map[string]*roomState, len(s.rooms))
	for id, st := range s.rooms {
		out[id] = st
	}
	return out
}

// cacheRoom puts st into the cache as roomID. ok is false, and st is not
// cached, when another state is cached under that id already: a load that
// raced a command or the follower keeps the copy that was there first.
func (s *Service) cacheRoom(roomID string, st *roomState) bool {
	s.reg.Lock()
	defer s.reg.Unlock()
	if _, ok := s.rooms[roomID]; ok {
		return false
	}
	s.rooms[roomID] = st
	return true
}

// uncacheRoom takes roomID out of the cache when st is the cached state, and
// drops the room's alert key if the index points at it (no hand-off to
// another room: see releaseAlertKeyLocked for that). It reports whether st
// was cached.
func (s *Service) uncacheRoom(roomID string, st *roomState) bool {
	s.reg.Lock()
	defer s.reg.Unlock()
	if s.rooms[roomID] != st {
		return false
	}
	if st.dedupKey != "" && s.alertRooms[st.dedupKey] == roomID {
		delete(s.alertRooms, st.dedupKey)
	}
	delete(s.rooms, roomID)
	return true
}

// isClosed reports whether Close has begun.
func (s *Service) isClosed() bool {
	s.reg.Lock()
	defer s.reg.Unlock()
	return s.closed
}

// addWorker reserves a place in s.wg for one goroutine the caller is about to
// start, unless Close has begun. The check and the Add are one step under
// reg, where Close sets closed before it waits, so Close's Wait never misses
// a goroutine and no Add can follow the Wait. The goroutine calls s.wg.Done;
// a caller that ends up starting nothing calls it itself.
func (s *Service) addWorker() bool { return s.addWorkers(1) }

// addWorkers is addWorker for n goroutines at once.
func (s *Service) addWorkers(n int) bool {
	s.reg.Lock()
	defer s.reg.Unlock()
	if s.closed {
		return false
	}
	s.wg.Add(n)
	return true
}

// dedupKeyOf returns the alert index key st was opened under ("" for none).
// The key lives in the registry because the index hands it between rooms
// (releaseAlertKeyLocked).
func (s *Service) dedupKeyOf(st *roomState) string {
	s.reg.Lock()
	defer s.reg.Unlock()
	return st.dedupKey
}

// alertRoomFor returns the room the alert index holds for indexKey ("" for
// none) and its cached state (nil when it is not cached).
func (s *Service) alertRoomFor(indexKey string) (string, *roomState) {
	s.reg.Lock()
	defer s.reg.Unlock()
	id := s.alertRooms[indexKey]
	if id == "" {
		return "", nil
	}
	return id, s.rooms[id]
}

// setAlertKey records st's index key and, when force is true or no room holds
// the key yet, points the index at roomID.
func (s *Service) setAlertKey(roomID string, st *roomState, key string, force bool) {
	s.reg.Lock()
	defer s.reg.Unlock()
	st.dedupKey = key
	if _, taken := s.alertRooms[key]; force || !taken {
		s.alertRooms[key] = roomID
	}
}

// published returns roomID's published snapshot, nil when the room is not
// cached or not published yet (a create in progress). It takes no room lock:
// the snapshot is shared and read-only, so a caller that hands it out clones
// it.
func (s *Service) published(roomID string) *RoomSnapshot {
	st := s.cached(roomID)
	if st == nil {
		return nil
	}
	return st.pub.Load()
}

// publishedRooms returns the published snapshot of every cached room that has
// one, by id; shared and read-only, like published.
func (s *Service) publishedRooms() map[string]*RoomSnapshot {
	rooms, _ := s.roomsForPipeline("")
	return rooms
}

// roomsForPipeline is publishedRooms, plus the ids of the cached rooms that
// are still being opened for pipelineID here (cached, not yet published).
// With pipelineID "" no room counts as opening.
func (s *Service) roomsForPipeline(pipelineID string) (published map[string]*RoomSnapshot, opening []string) {
	s.reg.Lock()
	defer s.reg.Unlock()
	published = make(map[string]*RoomSnapshot, len(s.rooms))
	for id, st := range s.rooms {
		if p := st.pub.Load(); p != nil {
			published[id] = p
		} else if pipelineID != "" && st.openingFor == pipelineID {
			opening = append(opening, id)
		}
	}
	return published, opening
}
