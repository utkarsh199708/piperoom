import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import type { AgentVerificationPayload } from '../../api/types'
import { VerificationCard } from './VerificationCard'

const base: AgentVerificationPayload = {
  proposal_id: 'p_1',
  recovered: true,
  verdict: 'recovered: every layer check passed and data is flowing again',
  checks: [
    { layer: 'kafka-connect', name: 'connect_status', ok: true, detail: 'RUNNING' },
    { layer: 'schema-registry', name: 'schema_compat', ok: false, detail: 'x'.repeat(200) },
  ],
  data_flow: {
    checked: true,
    fresh: true,
    sinks: [{ table: 'analytics.orders', newest: '2026-10-11T10:12:04Z', baseline: '2026-10-11T10:04:00Z', passed: true, passed_by: 'rows since proposal' }],
  },
}
const nudge = { roomOpen: true, isDriver: true, driver: { id: 'arjun', name: 'Arjun' }, onClose: vi.fn() }

describe('VerificationCard', () => {
  it('shows the verdict, each check with an accessible mark, and the sink line', () => {
    render(<VerificationCard verification={base} title="Restart task" nudge={nudge} />)
    expect(screen.getByRole('status', { name: '' })).toBeTruthy()
    expect(screen.getByText('Verified · recovered')).toBeInTheDocument()
    expect(screen.getByText(base.verdict)).toBeInTheDocument()
    expect(screen.getByLabelText('passed')).toBeInTheDocument()
    expect(screen.getByLabelText('failed')).toBeInTheDocument()
    expect(screen.getByText(/analytics\.orders · newest .* · after baseline .* · rows since proposal ✓/)).toBeInTheDocument()
  })

  it('clamps a long detail behind Show more', () => {
    render(<VerificationCard verification={base} title="t" nudge={nudge} />)
    const more = screen.getByRole('button', { name: 'Show more' })
    expect(more).toHaveAttribute('aria-expanded', 'false')
    fireEvent.click(more)
    expect(screen.getByRole('button', { name: 'Show less' })).toBeInTheDocument()
  })

  it('says data flow was not verified when it was not checked', () => {
    render(<VerificationCard verification={{ ...base, data_flow: { checked: false, fresh: false, detail: 'no warehouse reader' } }} title="t" nudge={nudge} />)
    expect(screen.getByText('Data flow not verified: no warehouse reader')).toBeInTheDocument()
  })

  it('not recovered: destructive heading, no close nudge', () => {
    render(<VerificationCard verification={{ ...base, recovered: false, verdict: 'NOT recovered' }} title="t" nudge={nudge} />)
    expect(screen.getByText('Verified · not recovered')).toBeInTheDocument()
    expect(screen.queryByText('Pipeline recovered.')).not.toBeInTheDocument()
  })

  it('a repeat is one muted line', () => {
    render(<VerificationCard verification={{ ...base, repeat: true, checks: [] }} title="t" nudge={nudge} />)
    expect(screen.getByText('Already verified · no checks re-run')).toBeInTheDocument()
    expect(screen.queryByText('Verified · recovered')).not.toBeInTheDocument()
  })

  it('the driver closes with the title as the resolution; others see who can; nobody driving says take control', () => {
    const onClose = vi.fn()
    const first = render(<VerificationCard verification={base} title="Restart task" nudge={{ ...nudge, onClose }} />)
    fireEvent.click(screen.getByRole('button', { name: /Close room/ }))
    expect(onClose).toHaveBeenCalledWith('Fixed by "Restart task"')
    first.unmount()
    const second = render(<VerificationCard verification={base} title="t" nudge={{ roomOpen: true, isDriver: false, driver: { id: 'arjun', name: 'Arjun' } }} />)
    expect(screen.getByText(/can close the room/)).toHaveTextContent(/Arjun.* can close the room/)
    second.unmount()
    render(<VerificationCard verification={base} title="t" nudge={{ roomOpen: true, isDriver: false }} />)
    expect(screen.getByText('Take control to close')).toBeInTheDocument()
  })

  it('no nudge once the room is closed', () => {
    render(<VerificationCard verification={base} title="t" nudge={{ ...nudge, roomOpen: false }} />)
    expect(screen.queryByText('Pipeline recovered.')).not.toBeInTheDocument()
  })
})

describe('VerificationCard third states', () => {
  // recovered=false also covers "nothing could be checked" and "inconclusive": neither is a verified failure.
  it.each([
    ['not verified: the outcome of proposal "p_1" step 1 is unknown; recovery is not claimed.', 'Not verified'],
    ['inconclusive: no source changes observed', 'Inconclusive'],
    ['NOT recovered: a layer check did not pass.', 'Verified · not recovered'],
  ])('%j heads the card %j', (verdict, heading) => {
    render(<VerificationCard verification={{ ...base, recovered: false, verdict }} title="t" nudge={nudge} />)
    expect(screen.getByText(heading)).toBeInTheDocument()
    expect(screen.queryByText('Pipeline recovered.')).toBeNull()
    if (heading !== 'Verified · not recovered') expect(screen.queryByText(/^Verified ·/)).toBeNull()
  })
})
