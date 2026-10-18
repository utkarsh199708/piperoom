import { describe, expect, it } from 'vitest'
import { DAG_PLACEHOLDER, PIPELINE_PLACEHOLDER, shellQuote, testAlertCommand } from './firstRun'

describe('testAlertCommand', () => {
  it('is the quickstart command with the blanks filled', () => {
    const a = testAlertCommand('http://127.0.0.1:8090', 'arjun', [
      { id: 'orders', title: 'Orders', shadow: false, kind: 'cdc' },
      { id: 'payments', title: '', shadow: false },
    ])
    expect(a.command).toBe(
      `curl -X POST http://127.0.0.1:8090/api/alerts/webhook -H 'Content-Type: application/json' \\\n` +
        `  -H 'X-User: arjun' -d '{"title":"test","pipeline_id":"orders"}'`,
    )
    expect(a.pipeline?.id).toBe('orders')
    expect(a.batch).toBe(false)
  })

  it('treats a pipeline without kind (an older server) as cdc', () => {
    const a = testAlertCommand('http://localhost:8090', 'u', [{ id: 'orders', title: '', shadow: false }])
    expect(a.batch).toBe(false)
    expect(a.command).not.toContain('dag_id')
  })

  it('adds dag_id for a batch pipeline, after pipeline_id', () => {
    const a = testAlertCommand('http://localhost:8090', 'u', [{ id: 'nightly', title: '', shadow: false, kind: 'batch' }])
    expect(a.batch).toBe(true)
    expect(a.command).toContain(`'{"title":"test","pipeline_id":"nightly","dag_id":"${DAG_PLACEHOLDER}"}'`)
  })

  it.each([
    ['no pipelines', []],
    ['pipelines not loaded', null],
  ])('uses the placeholder with %s', (_name, list) => {
    const a = testAlertCommand('http://localhost:8090/', 'u', list)
    expect(a.pipeline).toBeNull()
    expect(a.command).toContain(`"pipeline_id":"${PIPELINE_PLACEHOLDER}"`)
    // a trailing slash on the origin never doubles up
    expect(a.command).toContain('http://localhost:8090/api/alerts/webhook')
  })

  it('quotes ids from the files so the shell sees one word', () => {
    const a = testAlertCommand('http://localhost:8090', "o'brien", [{ id: "x'y", title: '', shadow: false }])
    expect(a.command).toContain(`-H 'X-User: o'\\''brien'`)
    expect(a.command).toContain(`'{"title":"test","pipeline_id":"x'\\''y"}'`)
  })
})

describe('shellQuote', () => {
  it.each([
    ['plain', "'plain'"],
    ['', "''"],
    ["a'b", "'a'\\''b'"],
    ['$(rm -rf /)', "'$(rm -rf /)'"],
  ])('%s', (input, want) => {
    expect(shellQuote(input)).toBe(want)
  })
})
