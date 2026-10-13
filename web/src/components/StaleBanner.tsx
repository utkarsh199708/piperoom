// Shown after a 409 params_mismatch on approve or reject (auth PR 4): the params_hash
// the UI sent is not the proposal's, so the screen is out of date. The room is
// already reloading; nothing is retried, the person looks again and decides again.
export const STALE_TEXT = 'This fix changed since you loaded it. Reload to see the new version.'

export function StaleBanner({ onReload, onDismiss }: { onReload: () => void; onDismiss: () => void }) {
  return (
    <div className="banner banner--error banner--stale" role="alert">
      <span>{STALE_TEXT}</span>
      <span className="banner__actions">
        <button type="button" className="btn btn--small" onClick={onReload}>
          Reload
        </button>
        <button type="button" className="btn btn--small btn--ghost" onClick={onDismiss}>
          Dismiss
        </button>
      </span>
    </div>
  )
}
