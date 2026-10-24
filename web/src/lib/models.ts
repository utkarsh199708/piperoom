// Pure helpers for the Model panel and the first-run picker (V8; docs/api.md "Models").
import { ApiClientError } from '../api/client'
import type { ModelPanel, ModelPrice, ModelRole, SupportedModel } from '../api/types'
import { errorText } from './errors'

export const NOT_MEASURED = 'not measured yet'

/** A Go server encodes an empty slice as `null`. */
export function roles(p: ModelPanel): readonly ModelRole[] {
  return p.roles ?? []
}

export function supportedList(p: ModelPanel): readonly SupportedModel[] {
  return p.supported ?? []
}

/** True when this server can change the model (standalone or quickstart); a team server is read-only. */
export function isEditable(p: ModelPanel): boolean {
  return p.mode === 'standalone' || p.mode === 'quickstart'
}

/** The picker opens by itself only here: an editable server with no usable model. */
export function needsFirstRun(p: ModelPanel): boolean {
  return isEditable(p) && p.state === 'no_model'
}

/** 0.81 → "81%"; null → "not measured yet". */
export function percent(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return NOT_MEASURED
  return `${Math.round(v * 1000) / 10}%`
}

/** 0 → "$0.00 per run" (a local model costs nothing); null → "not measured yet". */
export function costPerRun(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return NOT_MEASURED
  return `$${v.toFixed(2)} per run`
}

const usd = (n: number) => `$${Number(n.toFixed(4))}`

/** "$0.15 in · $0.6 out per 1M tokens"; null price is "not priced". */
export function priceText(p: ModelPrice | null | undefined): string {
  if (!p) return 'Not priced'
  return `${usd(p.input)} in · ${usd(p.output)} out per 1M tokens`
}

/** The provider as a person reads it: "openai (groq)". */
export function providerLabel(m: { provider: string; flavor?: string }): string {
  return m.flavor ? `${m.provider} (${m.flavor})` : m.provider
}

export const ROLE_LABEL: Record<string, string> = {
  main: 'Main investigation',
  triage: 'Triage',
  first_look: 'First look',
  summary: 'Hand-off summary',
  judge: 'Judge',
}

export function roleLabel(role: string): string {
  return Object.hasOwn(ROLE_LABEL, role) ? ROLE_LABEL[role] : role
}

export const STATUS_TEXT: Record<string, string> = {
  reference: 'Reference model',
  supported: 'Supported',
  experimental: 'Experimental',
  unverified: 'Unverified model',
}

/** Same key for a list entry and the role it selected. */
export function entryKey(m: { provider: string; flavor?: string; model: string }): string {
  return JSON.stringify([m.provider, m.flavor ?? '', m.model])
}

/**
 * A key is asked again whenever the target changes: a stored key is bound to the provider, flavor and the
 * whole base URL it was entered with (docs/api.md). Any typed base URL counts as a change, since the panel
 * knows the current host only. With no current main role nothing is stored yet.
 */
export function keyRequired(
  current: ModelRole | undefined,
  next: { provider: string; flavor?: string; baseUrl: string },
): boolean {
  if (!current) return true
  if (next.baseUrl.trim() !== '') return true
  return current.provider !== next.provider || (current.flavor ?? '') !== (next.flavor ?? '')
}

/** Flavors that run on the person's own machine: they usually need no key. */
export function usuallyKeyless(flavor: string | undefined): boolean {
  return flavor === 'ollama' || flavor === 'vllm'
}

export type PickerField = 'model' | 'base_url' | 'key' | 'form'

export interface PickerError {
  field: PickerField
  message: string
}

/** The field a 400 validation message names (it names the field, never the value). */
export function validationField(message: string): PickerField {
  if (/base[ _-]?url/i.test(message)) return 'base_url'
  if (/\bkey\b/i.test(message)) return 'key'
  if (/provider|flavor|model/i.test(message)) return 'model'
  return 'form'
}

/** Where a failed PUT /api/models is shown. 400 key_required and 400 validation sit next to their field. */
export function pickerError(e: unknown): PickerError {
  if (e instanceof ApiClientError) {
    if (e.status === 400 && e.code === 'key_required') {
      return {
        field: 'key',
        message: 'Enter the key for this provider. A stored key is never sent to a different provider or address.',
      }
    }
    if (e.status === 400 && e.code === 'validation') return { field: validationField(e.message), message: e.message }
    if (e.status === 404) {
      return { field: 'form', message: 'This is a team server. The model is set by whoever runs it (environment or Helm values).' }
    }
    if (e.status === 403 && e.code === 'origin') {
      return { field: 'form', message: 'Open Piperoom on the address the server printed and try again. This form only works from its own page.' }
    }
    if (e.status === 403 && e.code === 'viewer') return { field: 'form', message: 'You have read-only access, so you cannot change the model.' }
    if (e.status === 403) return { field: 'form', message: 'Only the person running this standalone server, on this machine, can change the model.' }
  }
  return { field: 'form', message: errorText(e) }
}
