// Why the sign-in screen appeared while a room was open. Written only by the session_expired
// path (never by a logout the person chose), so the person is told their tab was signed out.
export interface SigninNotice {
  kind: 'expired' | 'replaced'
  title?: string
}

const KEY = 'piperoom.signin.notice'

// The room on screen, kept by RoomScreen so the notice can name it.
let roomTitle: string | undefined
export function rememberRoomTitle(title: string | undefined) {
  roomTitle = title
}

function onRoomRoute(): boolean {
  return /^#\/rooms\//.test(window.location.hash)
}

/** Builds the notice for a session that ended on a room route (null elsewhere) and stores it. */
export function noticeForExpiredSession(devMode: boolean): SigninNotice | null {
  if (!onRoomRoute()) return null
  const notice: SigninNotice = { kind: devMode ? 'replaced' : 'expired', title: roomTitle }
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
