// First-run panel (P1e-2): the exact test alert for the empty room list. The command is the one in
// docs/pilot/quickstart.md §3 (and setup.md §6) with the blanks filled from this server: its own
// origin, the signed-in user for the dev-mode `X-User` header, and the first pipeline of
// GET /api/pipelines. Nothing in it is a secret: dev login has no password and the ids are public.
import type { PipelineInfo } from '../api/types'

/** The pipeline blank when the server knows none: the same placeholder the quickstart uses. */
export const PIPELINE_PLACEHOLDER = '<PIPELINE>'
/** The DAG blank of the batch variant: GET /api/pipelines does not list a pipeline's DAGs. */
export const DAG_PLACEHOLDER = '<DAG_ID>'

export const WEBHOOK_PATH = '/api/alerts/webhook'

// One shell word: single quotes, and a single quote inside becomes '\''. The values come from
// users.yaml and owners.yaml, so they are never trusted to be shell-safe.
export function shellQuote(s: string): string {
  return `'${s.replace(/'/g, `'\\''`)}'`
}

export interface TestAlert {
  /** The command to copy, two lines as in the quickstart. */
  command: string
  /** The pipeline the command names, or null when the placeholder is in it. */
  pipeline: PipelineInfo | null
  batch: boolean
}

/**
 * The `curl` for POST /api/alerts/webhook. A batch pipeline (`kind: "batch"`) also carries a
 * top-level `dag_id`: the server takes a valid `pipeline_id` first and only then looks for the one
 * batch pipeline whose `dags` list the id, so the command works with the placeholder left in, and
 * it shows the field an Airflow alert sends.
 */
export function testAlertCommand(origin: string, userId: string, pipelines: PipelineInfo[] | null): TestAlert {
  const pipeline = pipelines && pipelines.length > 0 ? pipelines[0] : null
  const batch = pipeline?.kind === 'batch'
  const body: Record<string, string> = { title: 'test', pipeline_id: pipeline?.id ?? PIPELINE_PLACEHOLDER }
  if (batch) body.dag_id = DAG_PLACEHOLDER
  const url = `${origin.replace(/\/+$/, '')}${WEBHOOK_PATH}`
  const command =
    `curl -X POST ${url} -H 'Content-Type: application/json' \\\n` +
    `  -H ${shellQuote(`X-User: ${userId}`)} -d ${shellQuote(JSON.stringify(body))}`
  return { command, pipeline, batch }
}
