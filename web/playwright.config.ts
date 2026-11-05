import { defineConfig } from '@playwright/test'

// Browser smoke test against the real server in --demo mode. The Go server
// serves both the API and the built UI (PIPEROOM_WEB_DIR), so the browser talks
// to one origin (docs/design/auth.md section 1.4): no Vite proxy, no cookie split.
//
// The command runs from web/ (the config's directory), so the relative paths are:
//   go run ../cmd/server          -> the repo's cmd/server
//   OWNERS_FILE ../dev/owners.yaml -> the repo's dev/owners.yaml
//   PIPEROOM_WEB_DIR dist           -> web/dist (built by `npm run build`)
export default defineConfig({
  testDir: './e2e',
  timeout: 90_000,
  expect: { timeout: 10_000 },
  // Retain a trace only for failures, so a flaky run is still debuggable.
  reporter: [['list']],
  use: {
    baseURL: 'http://127.0.0.1:8090',
    trace: 'retain-on-failure',
  },
  // The demo server (in-memory store, scripted agent). `--demo` turns the
  // watcher off, so the test opens the room by hand, exactly as the demo
  // runbook does (docs/demo/schema-change.md "No API key").
  webServer: {
    command: 'go run ../cmd/server --demo',
    url: 'http://127.0.0.1:8090/healthz',
    // First compile can be slow on a cold cache; fail rather than hang.
    timeout: 180_000,
    env: {
      PIPEROOM_HTTP_ADDR: '127.0.0.1:8090',
      PIPEROOM_WEB_DIR: 'dist',
      OWNERS_FILE: '../dev/owners.yaml',
      USERS_FILE: '../dev/users.yaml',
    },
  },
  projects: [
    { name: 'desktop', use: { viewport: { width: 1280, height: 800 } } },
    // The same flow again at a phone size (390x844).
    { name: 'mobile', use: { viewport: { width: 390, height: 844 } } },
  ],
})
