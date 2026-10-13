import { describe, expect, it } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { Avatar, Chip, Empty, Icon, Kicker, LayerTag, Panel, Skeleton, StatusPill, hueFor } from './ui'
import type { IconName } from './ui'

const ICONS: IconName[] = [
  'check', 'x', 'alert', 'clock', 'shield', 'lock', 'cloud-off', 'info', 'send',
  'copy', 'wheel', 'back', 'chevron-right', 'pipe', 'fingerprint', 'sun', 'moon',
]

describe('hueFor', () => {
  it('is stable and between 1 and 6', () => {
    for (const id of ['arjun', 'priya', 'meera', 'kabir', '', 'x'.repeat(200)]) {
      const h = hueFor(id)
      expect(h).toBe(hueFor(id))
      expect(h).toBeGreaterThanOrEqual(1)
      expect(h).toBeLessThanOrEqual(6)
    }
  })
})

describe('Icon', () => {
  it('draws every icon in the sprite, hidden from screen readers', () => {
    for (const name of ICONS) {
      const { container, unmount } = render(<Icon name={name} />)
      const svg = container.querySelector('svg')!
      expect(svg).toHaveAttribute('aria-hidden', 'true')
      expect(svg.children.length).toBeGreaterThan(0)
      expect(svg).toHaveAttribute('width', '16')
      unmount()
    }
  })
})

describe('Icon size', () => {
  it('sizes the svg inline when it is not the 16px default, so .icon CSS does not win', () => {
    const { container, rerender } = render(<Icon name="check" />)
    expect((container.querySelector('svg') as SVGElement).style.width).toBe('')
    rerender(<Icon name="check" size={40} className="big" />)
    const svg = container.querySelector('svg') as SVGElement
    expect(svg.style.width).toBe('40px')
    expect(svg.style.height).toBe('40px')
    expect(svg).toHaveClass('icon', 'big')
  })
})

describe('Avatar', () => {
  it('gets a hue class for people and a kind class for the agent and system', () => {
    const { container } = render(
      <>
        <Avatar actor={{ kind: 'human', id: 'arjun', name: 'Arjun' }} />
        <Avatar actor={{ kind: 'agent', id: 'agent', name: 'Agent' }} size="xs" />
        <Avatar actor={{ kind: 'system', id: 'sys', name: 'System' }} size="lg" />
      </>,
    )
    const [human, agent, system] = Array.from(container.querySelectorAll('.avatar'))
    expect(human.className).toContain(`avatar--h${hueFor('arjun')}`)
    expect(human.className).toContain('avatar--md')
    expect(agent.className).toContain('avatar--agent')
    expect(agent.className).toContain('avatar--xs')
    expect(system.className).toContain('avatar--system')
    expect(human).toHaveAttribute('title', 'Arjun')
    expect(human.textContent).toBe('AR')
  })

  it('shows the driver ring and wheel badge only for the driver', () => {
    const { container, rerender } = render(<Avatar actor={{ kind: 'human', id: 'a', name: 'Ann' }} />)
    expect(container.querySelector('.avatar--driver')).toBeNull()
    expect(container.querySelector('.avatar__badge')).toBeNull()
    rerender(<Avatar actor={{ kind: 'human', id: 'a', name: 'Ann' }} driver />)
    expect(container.querySelector('.avatar--driver')).not.toBeNull()
    expect(container.querySelector('.avatar__badge')).toHaveAttribute('title', 'driver')
  })
})

describe('Chip, Kicker, LayerTag', () => {
  it('Chip keeps its tone class, title and optional icon', () => {
    render(
      <Chip tone="warn" title="hint" icon="lock">
        View only
      </Chip>,
    )
    const chip = screen.getByText('View only')
    expect(chip).toHaveClass('chip', 'chip--warn')
    expect(chip).toHaveAttribute('title', 'hint')
    expect(chip.querySelector('svg')).not.toBeNull()
  })

  it('Kicker and LayerTag render their classes', () => {
    render(
      <>
        <Kicker>connector</Kicker>
        <LayerTag layer="kafka-connect" />
      </>,
    )
    expect(screen.getByText('connector')).toHaveClass('kicker')
    expect(screen.getByText('kafka-connect')).toHaveClass('layer-tag', 'layer-tag--kafka-connect')
  })
})

describe('Panel', () => {
  it('renders the head, title, count, right slot and body', () => {
    render(
      <Panel title="Who's here" count={3} right={<button>Act</button>} className="presence">
        body text
      </Panel>,
    )
    const title = screen.getByText("Who's here")
    expect(title.tagName).toBe('H2')
    expect(title).toHaveClass('panel__title')
    expect(within(title).getByText('3')).toHaveClass('panel__count')
    expect(screen.getByRole('button', { name: 'Act' }).parentElement).toHaveClass('panel__right')
    const panel = title.closest('section')!
    expect(panel).toHaveClass('panel', 'presence')
    expect(screen.getByText('body text')).toHaveClass('panel__body')
  })

  it('has no head without a title or right slot, and honours `as`', () => {
    const { container } = render(
      <Panel as="aside">
        just body
      </Panel>,
    )
    expect(container.querySelector('.panel__head')).toBeNull()
    expect(container.firstElementChild!.tagName).toBe('ASIDE')
  })
})

describe('Empty', () => {
  it('with only children is the old one-line paragraph', () => {
    render(<Empty>No open rooms.</Empty>)
    const p = screen.getByText('No open rooms.')
    expect(p.tagName).toBe('P')
    expect(p).toHaveClass('empty')
  })

  it('with icon and title shows the help line under the title', () => {
    const { container } = render(
      <Empty icon="shield" title="No fix proposed yet">
        The agent proposes a fix later.
      </Empty>,
    )
    expect(container.querySelector('.empty svg')).not.toBeNull()
    expect(screen.getByText('No fix proposed yet')).toHaveClass('empty__title')
    expect(screen.getByText('The agent proposes a fix later.')).toHaveClass('empty__hint')
  })
})

describe('Skeleton', () => {
  it('is an aria-hidden block with the given size', () => {
    const { container } = render(<Skeleton h={20} w={120} className="x" />)
    const el = container.firstElementChild as HTMLElement
    expect(el).toHaveClass('skeleton', 'x')
    expect(el).toHaveAttribute('aria-hidden', 'true')
    expect(el.style.height).toBe('20px')
    expect(el.style.width).toBe('120px')
  })
})

describe('StatusPill', () => {
  it('is a polite status region with the chip label', () => {
    render(<StatusPill state="working" detail="step 5" />)
    const pill = screen.getByRole('status')
    expect(pill).toHaveAttribute('aria-live', 'polite')
    expect(pill).toHaveTextContent('working · step 5')
    expect(pill).toHaveClass('status-pill--agent')
    expect(pill.querySelector('.status-pill__dot')).toHaveClass('is-live')
  })

  it('is live while waiting for approval, calm otherwise', () => {
    const { container, rerender } = render(<StatusPill state="waiting_approval" />)
    expect(container.querySelector('.status-pill--warn .is-live')).not.toBeNull()
    rerender(<StatusPill state="idle" />)
    expect(container.querySelector('.is-live')).toBeNull()
  })

  it('shows the resolved and error words with a glyph, not a dot', () => {
    const { container, rerender } = render(<StatusPill state="done" />)
    expect(screen.getByText('resolved — verified')).toBeInTheDocument()
    expect(container.querySelector('.status-pill--ok svg')).not.toBeNull()
    rerender(<StatusPill state="error" />)
    expect(container.querySelector('.status-pill--fail svg')).not.toBeNull()
    expect(container.querySelector('.status-pill__dot')).toBeNull()
  })
})
