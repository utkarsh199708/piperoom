// Swaps the tab icon for one with an amber dot while something needs this person.
const PLAIN = '/favicon.svg'
const ALERT = '/favicon-alert.svg'

/** Idempotent; does nothing when the page has no #favicon link (tests, embeds). */
export function setFavicon(alert: boolean): void {
  const link = document.getElementById('favicon')
  if (!(link instanceof HTMLLinkElement)) return
  const href = alert ? ALERT : PLAIN
  if (link.getAttribute('href') !== href) link.setAttribute('href', href)
}
