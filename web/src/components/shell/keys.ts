/** An input, textarea, select or contenteditable element (or something inside one). */
export function isEditableTarget(target: EventTarget | null): boolean {
  if (!(target instanceof Element)) return false
  return target.closest('input, textarea, select, [contenteditable=""], [contenteditable="true"]') !== null
}
