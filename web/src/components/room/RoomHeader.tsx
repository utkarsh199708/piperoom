import { Check, CircleCheck, Eye, Repeat2, UserPlus } from 'lucide-react'
import type { RoomState } from '@/state/types'
import type { CurrentUser } from '@/state/permissions'
import { canClose, canHandoff, isViewer } from '@/state/permissions'
import type { ConnState } from '@/hooks/useRoom'
import { ageLabel, clockTime } from '@/lib/time'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Kbd } from '@/components/ui/kbd'
import { AgentPill } from './AgentPill'
import { PersonAvatar } from './PersonAvatar'
import { StatusDot } from './StatusDot'
import type { DotTone } from './StatusDot'

export interface HeaderMember {
  id: string
  name: string
}

const MAX_AVATARS = 4

// "Kabir · driving", "Arjun · you", "Arjun · you, driving".
function avatarLabel(name: string, you: boolean, driver: boolean): string {
  if (you) return driver ? `${name} · you, driving` : `${name} · you`
  return driver ? `${name} · driving` : name
}

function Sep() {
  return (
    <span aria-hidden="true" className="text-subtle-foreground">
      ·
    </span>
  )
}

// Title row (name, severity, status, then the agent pill and the room actions on the
// right), a mono facts line, and who is here with the driver ringed.
// The actions live in the page, not in the shell's top bar, so they also work where
// there is no shell and they wrap under the title on a phone.
export function RoomHeader({
  state,
  user,
  members,
  conn,
  copied,
  onHandoff,
  onCloseRoom,
  onInvite,
}: {
  state: RoomState
  user: CurrentUser
  members: HeaderMember[]
  conn: ConnState
  copied: boolean
  onHandoff: () => void
  onCloseRoom: () => void
  onInvite: () => void
}) {
  const open = state.status === 'open'
  const dot: DotTone = !open ? 'ok' : state.severity === 'sev1' ? 'fail' : state.severity === 'sev2' ? 'warn' : 'muted'
  const ordered = [...members].sort((a, b) => Number(b.id === state.driver?.id) - Number(a.id === state.driver?.id))
  const shown = ordered.slice(0, MAX_AVATARS)
  const extra = ordered.length - shown.length

  return (
    <header className="flex flex-col gap-2 px-4 pt-4 sm:px-6">
      <div className="flex flex-wrap items-center gap-x-2.5 gap-y-2">
        <h1 className="m-0 min-w-0 break-words text-xl font-semibold tracking-tight [overflow-wrap:anywhere] [unicode-bidi:isolate]">
          {state.title}
        </h1>
        <Badge variant={state.severity} className="uppercase">
          {state.severity}
        </Badge>
        {state.alert && (
          <Badge variant="info" title="This room was opened by an alert webhook">
            {state.alert.source ? `alert · ${state.alert.source}` : 'alert'}
          </Badge>
        )}
        <Badge variant="outline">
          <StatusDot tone={dot} />
          {open ? `Open · ${ageLabel(state.opened_at) || 'now'}` : 'Closed'}
        </Badge>

        <div className="ml-auto flex flex-wrap items-center gap-2">
          {conn === 'reconnecting' && <Badge variant="warning">reconnecting…</Badge>}
          {conn === 'connecting' && <Badge variant="info">connecting…</Badge>}
          {isViewer(user) && (
            <span
              title="You can read rooms but not change anything"
              className="inline-flex h-6 items-center gap-1.5 whitespace-nowrap rounded-full border border-border bg-card px-2 text-xs font-medium text-muted-foreground"
            >
              <Eye className="size-3" aria-hidden="true" />
              View only
            </span>
          )}
          <AgentPill state={state} />
          {canHandoff(state, user) && (
            <Button variant="secondary" aria-keyshortcuts="H" onClick={onHandoff}>
              <Repeat2 aria-hidden="true" />
              Hand off
              <Kbd aria-hidden="true">H</Kbd>
            </Button>
          )}
          {canClose(state, user) && (
            <Button variant="outline" aria-keyshortcuts="C" onClick={onCloseRoom}>
              <CircleCheck aria-hidden="true" />
              Close room
              <Kbd aria-hidden="true">C</Kbd>
            </Button>
          )}
          <Button
            variant="ghost"
            size="icon"
            onClick={onInvite}
            aria-label={copied ? 'Copied' : 'Invite (copy link)'}
            title={copied ? 'Link copied' : 'Invite: copy the link to this room'}
          >
            {copied ? <Check aria-hidden="true" /> : <UserPlus aria-hidden="true" />}
          </Button>
        </div>
      </div>

      <p className="m-0 flex flex-wrap items-center gap-x-2.5 font-mono text-xs text-subtle-foreground">
        {state.pipeline_id && (
          <>
            <span>{state.pipeline_id}</span>
            <Sep />
          </>
        )}
        <span>#{state.id}</span>
        {state.opened_at && (
          <>
            <Sep />
            <span>opened {clockTime(state.opened_at)}</span>
          </>
        )}
        {state.closed_at && (
          <>
            <Sep />
            <span>closed {clockTime(state.closed_at)}</span>
          </>
        )}
      </p>

      <div className="flex flex-wrap items-center gap-2.5">
        {shown.length > 0 && (
          <ul aria-label="People in the room" className="m-0 flex list-none items-center p-0">
            {shown.map((m, i) => {
              const driver = m.id === state.driver?.id
              return (
                <li key={m.id} className={i === 0 ? '' : '-ml-1.5'}>
                  <PersonAvatar
                    actor={{ id: m.id, name: m.name }}
                    driver={driver}
                    stacked
                    label={avatarLabel(m.name, m.id === user.id, driver)}
                    className={driver ? 'ring-offset-0' : undefined}
                  />
                </li>
              )
            })}
            {extra > 0 && (
              <li className="-ml-1.5">
                <span className="relative flex size-6 items-center justify-center rounded-full border-2 border-background bg-muted text-[10px] font-semibold text-muted-foreground">
                  +{extra}
                </span>
              </li>
            )}
          </ul>
        )}
        <span className="min-w-0 break-words text-muted-foreground [overflow-wrap:anywhere]" data-testid="driver-line">
          {/* <bdi>: a name with a bidi override must not reorder the words after it. */}
          {state.driver ? (
            <>
              <bdi>{state.driver.name}</bdi> is driving
            </>
          ) : (
            'No driver'
          )}
        </span>
        <Sep />
        <span className="text-subtle-foreground">{members.length} here</span>
      </div>
    </header>
  )
}
