// Browser notifications and the tab-title badge. Permission is only ever asked from
// requestNotifyPermission(), which a button click calls; nothing here asks on load.
export type NotifyPermission = 'unsupported' | 'default' | 'granted' | 'denied'

export function notifyPermission(): NotifyPermission {
  if (typeof Notification === 'undefined') return 'unsupported'
  return Notification.permission
}

export async function requestNotifyPermission(): Promise<NotifyPermission> {
  if (typeof Notification === 'undefined') return 'unsupported'
  try {
    return await Notification.requestPermission()
  } catch {
    return notifyPermission()
  }
}

/** Shows a notification when allowed. Returns false when it could not. */
export function showNotification(title: string, body: string, tag: string): boolean {
  if (notifyPermission() !== 'granted') return false
  try {
    new Notification(title, { body, tag })
    return true
  } catch {
    return false
  }
}

const BADGE = /^\(\d+\)\s+/

/** "(3) Piperoom" for a count above 0, the plain title for 0. Idempotent. */
export function withBadge(title: string, count: number): string {
  const plain = title.replace(BADGE, '')
  return count > 0 ? `(${count}) ${plain}` : plain
}
