// Rules behind the setup controls (U1-12b; docs/api.md "Setup rooms" and
// docs/design/setup-agent.md §3.5–§3.7). Pure, so every state is a table test: which classes a
// raise may offer, who approves registration, where the pipeline will be watched, the secret
// field's first pass and the maintenance window's bounds. The server stays the authority.
import type { SetupPipelineExtendBody } from '../api/types'
import { splitNames } from './setupForm'

/** Lowest to highest. A person may raise a target's class and never lower it (§3.5). */
export const SETUP_CLASS_ORDER = ['local', 'shared', 'production'] as const
export type SetupClass = (typeof SETUP_CLASS_ORDER)[number]

/** The server never lets a registered target sit below `shared`, so "" and unknown read as that. */
export function classRank(cls: string): number {
  const index = SETUP_CLASS_ORDER.indexOf(cls as SetupClass)
  return index < 0 ? SETUP_CLASS_ORDER.indexOf('shared') : index
}

/** The class a raise starts from: an absent one is the server's floor, `shared`. */
export function effectiveClass(cls: string): SetupClass {
  return SETUP_CLASS_ORDER[classRank(cls)]
}

/** The classes a raise may offer: strictly higher than the target's current class. */
export function classesAbove(current: string): SetupClass[] {
  const floor = classRank(current)
  return SETUP_CLASS_ORDER.filter((cls) => classRank(cls) > floor)
}

/** The highest class among the room's targets; [] reads as the server's default, `shared`. */
export function highestClass(targets: readonly { class?: string }[]): SetupClass {
  if (targets.length === 0) return 'shared'
  return targets.reduce<SetupClass>(
    (highest, target) =>
      classRank(target.class ?? '') > classRank(highest) ? effectiveClass(target.class ?? '') : highest,
    'local',
  )
}

/**
 * Who watches the registered pipeline (§3.7): a pipeline with any target above `local` is read
 * by this server; one that is local throughout is watched by the standalone server on the same
 * machine. `registered` ("server" | "local") wins once pipeline.registered landed.
 */
export function watchedByFor(
  targets: readonly { class?: string }[],
  registered?: string,
): 'server' | 'local' {
  if (registered === 'server' || registered === 'local') return registered
  if (targets.length === 0) return 'server'
  return targets.every((target) => effectiveClass(target.class ?? '') === 'local') ? 'local' : 'server'
}

/** The registration card's "where it will be watched" sentence. */
export function watchedByCopy(by: 'server' | 'local', registered: boolean): string {
  if (by === 'local') {
    return registered
      ? 'Watched locally while piperoom-setup runs.'
      : 'Watched locally while piperoom-setup runs — a team server cannot reach 127.0.0.1.'
  }
  return registered
    ? 'Watched by this server.'
    : 'Watched by this server once it is registered: the server reads it through the read-only credentials the runner registers.'
}

/**
 * The registration card's "who approves" sentence (§3.6). Before registration every layer of the
 * provisional pipeline is unowned, so only solo mode lets the driver approve it alone; a team
 * server wants a tenant admin (or an overlapping pipeline's owners) who is not the requester.
 */
export function approverCopy(solo: boolean): string {
  return solo
    ? 'You approve it: this server is in solo mode, so your approval is the only one required.'
    : "A tenant admin, or the owners of a pipeline that overlaps this one, approves it — always someone other than the person asking. The driver's own approval does not count on a team server."
}

// docs/api.md: name matches ^[a-zA-Z_][a-zA-Z0-9_]{0,127}$; value >= 6 bytes and <= 4 KiB.
const SECRET_NAME = /^[a-zA-Z_][a-zA-Z0-9_]{0,127}$/

/** The secret name's first pass, or undefined. The server refuses the same shapes. */
export function secretNameError(name: string): string | undefined {
  if (!name) return 'A secret name is required.'
  if (!SECRET_NAME.test(name)) {
    return 'A secret name starts with a letter or underscore and holds letters, digits and underscores, at most 128 characters.'
  }
  return undefined
}

/** The secret value's first pass, in bytes as the server counts it, or undefined. */
export function secretValueError(value: string): string | undefined {
  if (!value) return 'A secret value is required.'
  const bytes = new TextEncoder().encode(value).length
  if (bytes < 6) return 'A secret value is at least 6 bytes.'
  if (bytes > 4096) return 'A secret value is at most 4 KiB.'
  return undefined
}

/** The four lists the pipeline-extend form edits; each is one name per line or comma. */
export interface SetupExtensionValues {
  connectors: string
  tables: string
  consumerGroups: string
  sinkTables: string
}

export const EMPTY_EXTENSION: SetupExtensionValues = {
  connectors: '',
  tables: '',
  consumerGroups: '',
  sinkTables: '',
}

/** POST /api/rooms/{id}/setup/pipeline's body with only the lists that name something, else null. */
export function extensionBody(values: SetupExtensionValues): SetupPipelineExtendBody | null {
  const body: SetupPipelineExtendBody = {}
  const connectors = splitNames(values.connectors)
  const tables = splitNames(values.tables)
  const consumerGroups = splitNames(values.consumerGroups)
  const sinkTables = splitNames(values.sinkTables)
  if (connectors.length > 0) body.add_connectors = connectors
  if (tables.length > 0) body.add_tables = tables
  if (consumerGroups.length > 0) body.add_consumer_groups = consumerGroups
  if (sinkTables.length > 0) body.add_sink_tables = sinkTables
  return Object.keys(body).length > 0 ? body : null
}

/** A field-level problem in a control's form; the input with `field` takes aria-invalid. */
export class SetupControlError extends Error {
  field: string
  constructor(field: string, message: string) {
    super(message)
    this.name = 'SetupControlError'
    this.field = field
  }
}

/**
 * datetime-local values to the RFC 3339 pair POST /api/rooms/{id}/setup/maintenance-window takes.
 * A window is at most 4 hours, starts at most 24 hours ahead, and ends in the future
 * (docs/api.md; design §3.5).
 */
export function windowFromInputs(
  start: string,
  end: string,
  now: Date = new Date(),
): { start: string; end: string } {
  if (!start) throw new SetupControlError('start', 'Choose when the window starts.')
  if (!end) throw new SetupControlError('end', 'Choose when the window ends.')
  const from = new Date(start)
  const to = new Date(end)
  if (Number.isNaN(from.getTime())) throw new SetupControlError('start', 'That start is not a time.')
  if (Number.isNaN(to.getTime())) throw new SetupControlError('end', 'That end is not a time.')
  if (to.getTime() <= from.getTime()) {
    throw new SetupControlError('end', 'The window must end after it starts.')
  }
  if (to.getTime() - from.getTime() > 4 * 60 * 60 * 1000) {
    throw new SetupControlError('end', 'A window is at most four hours long.')
  }
  if (from.getTime() - now.getTime() > 24 * 60 * 60 * 1000) {
    throw new SetupControlError('start', 'A window starts at most 24 hours from now.')
  }
  if (to.getTime() <= now.getTime()) {
    throw new SetupControlError('end', 'The window must end in the future.')
  }
  return { start: from.toISOString(), end: to.toISOString() }
}
