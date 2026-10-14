import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { Checklist } from './Checklist'

const data = { hits: [{ section: 'Failed connector', items: ['Check status', 'Resume the connector', 'Verify lag'] }] }
const access = { canTick: true, canAsk: true }

function setup(over: Partial<React.ComponentProps<typeof Checklist>> = {}) {
  const onTick = vi.fn()
  const onAsk = vi.fn()
  render(<Checklist stepId="s1" toolUseId="tu1" data={data} done={{}} access={access} onTick={onTick} onAsk={onAsk} {...over} />)
  return { onTick, onAsk }
}

describe('Checklist', () => {
  it('has a labelled checkbox per item', () => {
    setup()
    expect(screen.getByRole('checkbox', { name: 'Resume the connector' })).not.toBeChecked()
    expect(screen.getAllByRole('checkbox')).toHaveLength(3)
  })

  it('ticks: sends the item and the new state', () => {
    const { onTick } = setup()
    fireEvent.click(screen.getByRole('checkbox', { name: 'Resume the connector' }))
    expect(onTick).toHaveBeenCalledWith({ step_id: 's1', tool_use_id: 'tu1', hit: 0, item: 1, done: true })
  })

  it('unticks a ticked item and shows who ticked it', () => {
    const { onTick } = setup({ done: { 'tu1:0:1': { by: { kind: 'human', id: 'kabir', name: 'Kabir' }, at: new Date().toISOString() } } })
    const box = screen.getByRole('checkbox', { name: 'Resume the connector' })
    expect(box).toBeChecked()
    expect(screen.getByText(/Kabir · just now/)).toBeInTheDocument()
    fireEvent.click(box)
    expect(onTick).toHaveBeenCalledWith(expect.objectContaining({ item: 1, done: false }))
  })

  it('disables the boxes and hides Ask for a viewer', () => {
    setup({ access: { canTick: false, canAsk: false } })
    for (const b of screen.getAllByRole('checkbox')) expect(b).toBeDisabled()
    expect(screen.queryByRole('button', { name: /Ask the agent/ })).toBeNull()
  })

  it('asks the agent with the step text', () => {
    const { onAsk } = setup()
    fireEvent.click(screen.getByRole('button', { name: 'Ask the agent to do step 3' }))
    expect(onAsk).toHaveBeenCalledWith('Do step 3 of "Failed connector": Verify lag')
  })

  it('renders nothing for a result without items', () => {
    const { container } = render(<Checklist stepId="s" toolUseId="t" data={{ hits: [{ section: 'x' }] }} done={{}} access={access} onTick={vi.fn()} onAsk={vi.fn()} />)
    expect(container).toBeEmptyDOMElement()
  })
})
