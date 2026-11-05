import { expect, test } from '@playwright/test'

const USER = 'arjun'
const TITLE = 'Restart the orders connector by hand'

// P3d-2 against the real server in --demo mode: the room's creator is its driver, opens "Propose a
// fix yourself" from the decision bar, picks a registered action, fills its generated params form
// and sends it. The card arrives with "authored by Arjun" and the server's preview fills in.
// Desktop only: the phone sheet keeps to the one decision it asks for and has no such button.
test('the driver proposes a fix from the decision bar', async ({ page, context, baseURL }, testInfo) => {
  test.skip(testInfo.project.name === 'mobile', 'the propose button is not on the phone sheet')
  const base = baseURL ?? 'http://127.0.0.1:8090'
  const pageErrors: string[] = []
  page.on('pageerror', (err) => pageErrors.push(err.message))

  const login = await context.request.post(`${base}/auth/dev-login`, { data: { user: USER } })
  expect(login.status()).toBe(200)
  const created = await context.request.post(`${base}/api/rooms`, {
    data: { title: 'Playwright propose a fix', severity: 'sev3', pipeline_id: 'orders' },
  })
  expect(created.status()).toBe(201)
  const room = (await created.json()) as { id: string }

  await page.goto(`${base}/#/rooms/${room.id}`)
  const bar = page.getByRole('region', { name: 'Decision' })
  await bar.getByRole('button', { name: 'Propose a fix yourself' }).click({ timeout: 30_000 })

  const dialog = page.getByRole('dialog', { name: 'Propose a fix yourself' })
  await dialog.getByRole('combobox', { name: 'Action' }).selectOption('connect_restart_connector')
  await dialog.getByRole('textbox', { name: 'connector' }).fill('pg-orders-connector')
  await dialog.getByRole('textbox', { name: 'Title' }).fill(TITLE)
  await dialog.getByRole('textbox', { name: 'Rollback' }).fill('restart the connector again')
  await dialog.getByRole('button', { name: /Propose fix/ }).click()
  await expect(dialog).toBeHidden()

  const card = page.locator('article').filter({ has: page.getByRole('heading', { name: TITLE }) })
  await expect(card).toBeVisible({ timeout: 20_000 })
  await expect(card.getByText('authored by')).toBeVisible()
  await expect(card).toContainText('authored by Arjun')
  await expect(card.getByRole('region', { name: 'What this touches' })).toBeVisible({ timeout: 20_000 })
  await expect(card.getByRole('region', { name: 'What this touches' })).toContainText('pg-orders-connector')

  expect(pageErrors).toEqual([])
})
