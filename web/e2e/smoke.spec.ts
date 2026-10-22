import { expect, test } from '@playwright/test'

const USER = 'arjun'
const ROOM_TITLE = 'Playwright smoke'

// One end-to-end smoke of the room UI against the real server in --demo mode:
// sign in, see the room list, open a room by hand, watch the scripted agent
// stream its steps, then the decision bar appears once it proposes the fix.
// No console errors or uncaught page errors are allowed.
test('room UI works against the demo server', async ({ page, context, baseURL }) => {
  const base = baseURL ?? 'http://127.0.0.1:8090'
  const consoleErrors: string[] = []
  const pageErrors: string[] = []
  page.on('console', (msg) => {
    if (msg.type() === 'error') consoleErrors.push(msg.text())
  })
  page.on('pageerror', (err) => pageErrors.push(err.message))

  // 1. Dev login. POST /auth/dev-login sets the HttpOnly session cookie;
  //    context.request shares the cookie jar with the browser, so the next
  //    page load is already signed in.
  const login = await context.request.post(`${base}/auth/dev-login`, { data: { user: USER } })
  expect(login.status()).toBe(200)

  // 2. The room list renders.
  await page.goto(`${base}/`)
  await expect(page.getByRole('heading', { level: 1, name: 'Rooms' })).toBeVisible()

  // 3. Open a demo room: --demo runs no watcher, so rooms open by hand with the
  //    same POST /api/rooms the new-room form makes (pipeline `orders`).
  const created = await context.request.post(`${base}/api/rooms`, {
    data: { title: ROOM_TITLE, severity: 'sev2', pipeline_id: 'orders' },
  })
  expect(created.status()).toBe(201)
  const room = (await created.json()) as { id: string }
  expect(room.id).toBeTruthy()

  // 4. Agent steps stream into the investigation feed (bounded wait). The step
  //    title also appears in the activity rail, so scope to the timeline.
  await page.goto(`${base}/#/rooms/${room.id}`)
  const investigation = page.locator('section[aria-labelledby="investigation-heading"]')
  await expect(investigation.getByText('Check the Debezium connector')).toBeVisible({ timeout: 20_000 })

  // 5. The decision bar appears once the agent writes the fix proposal
  //    (bounded wait: the scripted steps take ~8s).
  await expect(page.getByRole('region', { name: 'Decision' })).toBeVisible({ timeout: 30_000 })

  // 5b. The scripted what_changed step fills the "What changed" lane, so the
  //     demo shows the schema-change evidence above the investigation feed
  //     (KAN-399). It streams before the proposal, but wait after it so the
  //     step is certainly recorded in a replayed room too. The lane starts
  //     collapsed under 768 px, so open it before looking for a row.
  const lane = page.getByRole('region', { name: 'What changed' })
  await expect(lane).toBeVisible({ timeout: 20_000 })
  await expect(lane.getByText('3 changes')).toBeVisible()
  const laneToggle = lane.getByRole('button', { name: /What changed/ })
  if ((await laneToggle.getAttribute('aria-expanded')) === 'false') await laneToggle.click()
  await expect(lane.getByText('schema_version')).toBeVisible()
  await expect(lane.getByText('connector_config')).toBeVisible()

  // 6. The run must be clean.
  expect(pageErrors).toEqual([])
  expect(consoleErrors).toEqual([])
})
