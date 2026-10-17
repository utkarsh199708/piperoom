// QA F19: a step link scrolls to the step and keeps the route (HashRouter reads the fragment as the route).
import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { HashRouter, Route, Routes } from 'react-router-dom'
import type { StepState } from '@/state/types'
import { AgentText, StepNumbersProvider } from './AgentText'

const A = 's_01M4Y9X0ZPZ0NPJW00JGWVY8VP'
const steps: StepState[] = [{ id: A, title: 't', tool_calls: [], messages: [] }]

describe('AgentText step links', () => {
  it('click keeps the room route, scrolls to the step and highlights it', () => {
    window.location.hash = '#/rooms/r_x'
    const scroll = vi.fn()
    Element.prototype.scrollIntoView = scroll
    render(
      <HashRouter>
        <Routes>
          <Route path="/rooms/:id" element={<><li id={`step-${A}`} data-testid="step" /><StepNumbersProvider steps={steps}><AgentText text={`Checked ${A}`} /></StepNumbersProvider></>} />
          <Route path="*" element={<div>LIST</div>} />
        </Routes>
      </HashRouter>,
    )
    fireEvent.click(screen.getByRole('link', { name: 'step 1' }))
    expect(window.location.hash).toBe('#/rooms/r_x')
    expect(screen.queryByText('LIST')).toBeNull()
    expect(scroll).toHaveBeenCalled()
    expect(screen.getByTestId('step').className).toContain('ring-2')
  })
})
