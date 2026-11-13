// Why the sign-in screen appeared while a room was open. Written only by the session_expired
// path (never by a logout the person chose). The server answers 401 session_expired the same way
// for an unknown, expired, garbage or revoked cookie, so the client cannot tell why the session
// ended and the wording stays neutral.
export interface SigninNotice {
  kind: 'expired'
  title?: string
  /** The room the title belongs to; the title is dropped once the URL points at another room. */
  roomId?: string
}

const KEY = 'piperoom.signin.notice'

// The room on screen, kept by RoomScreen so the notice can name it.
let roomTitle: { roomId: string | undefined; title: string } | undefined
export function rememberRoomTitle(title: string | undefined) {
  roomTitle = title ? { roomId: currentRoomId(), title } : undefined
}

/** The room id in the URL hash (#/rooms/<id>), or undefined off a room route. */
export function currentRoomId(): string | undefined {
  const m = /^#\/rooms\/([^/?#]+)/.exec(window.location.hash)
  if (!m) return undefined
  try {
    return decodeURIComponent(m[1])
  } catch {
    return m[1]
  }
}

function onRoomRoute(): boolean {
  return currentRoomId() !== undefined
}

/** The notice as it should read for the URL now: a title that belongs to another room is dropped. */
export function noticeForRoute(notice: SigninNotice | null): SigninNotice | null {
  if (!notice?.title || notice.roomId === currentRoomId()) return notice
  return { kind: 'expired', roomId: currentRoomId() }
}

/** Builds the notice for a session that ended on a room route (null elsewhere) and stores it. */
export function noticeForExpiredSession(): SigninNotice | null {
  if (!onRoomRoute()) return null
  const roomId = currentRoomId()
  const title = roomTitle && roomTitle.roomId === roomId ? roomTitle.title : undefined
  const notice: SigninNotice = { kind: 'expired', title, roomId }
  try {
    sessionStorage.setItem(KEY, JSON.stringify(notice))
  } catch {
    // private mode: the in-memory copy still shows
  }
  return notice
}

export function clearSigninNotice() {
  try {
    sessionStorage.removeItem(KEY)
  } catch {
    // nothing to clear
  }
}
