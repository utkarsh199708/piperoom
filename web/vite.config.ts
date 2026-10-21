import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'
import { fileURLToPath, URL } from 'node:url'
import { DEVCTL_PROXY_KEY, devctlLoopbackGuard, devctlProxyOptions } from './src/lib/devctlProxy'

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  // Where the Go server listens. Override with PIPEROOM_API, for example
  // `PIPEROOM_API=http://127.0.0.1:9000 npm run dev`.
  const env = loadEnv(mode, process.cwd(), '')
  const api = process.env.PIPEROOM_API ?? env.PIPEROOM_API ?? 'http://127.0.0.1:8090'

  // Where the dev-only `devctl serve` break/fix API listens (loopback, no auth).
  // Override with PIPEROOM_DEVCTL, e.g. `PIPEROOM_DEVCTL=http://127.0.0.1:7071 npm run dev`.
  const devctl = process.env.PIPEROOM_DEVCTL ?? env.PIPEROOM_DEVCTL ?? 'http://127.0.0.1:7071'

  // The session is an HttpOnly cookie, so the browser must talk to ONE origin.
  // The dev server proxies /api and /auth to the Go server (docs/design/auth.md
  // section 1.4). `changeOrigin` stays off on purpose: the server then sees
  // `Host: localhost:5173`, which is on its loopback allow-list (any port).
  // `ws: true` carries the room WebSocket (/api/rooms/{id}/ws) with its cookie.
  const proxy = {
    '^/api(/|$)': { target: api, ws: true },
    '^/auth(/|$)': { target: api },
    // The Slack deep link: the server answers GET /rooms/{id} with a 302 to /#/rooms/{id}.
    '^/rooms/': { target: api },
  }

  return {
    plugins: [react(), devctlLoopbackGuard()],
    resolve: { alias: { '@': fileURLToPath(new URL('./src', import.meta.url)) } },
    build: {
      rollupOptions: {
        output: {
          // Keep the long-lived libraries out of the app chunk: they change rarely, so the
          // browser cache keeps them across releases, and each chunk stays under 500 kB.
          manualChunks(id: string) {
            if (!id.includes('node_modules')) return undefined
            if (/node_modules\/(react|react-dom|scheduler|react-router|react-router-dom)\//.test(id)) return 'react'
            if (/node_modules\/(@radix-ui|@floating-ui|aria-hidden|react-remove-scroll|react-style-singleton|use-callback-ref|use-sidecar)\b/.test(id)) return 'radix'
            return undefined
          },
        },
      },
    },
    server: {
      proxy: {
        ...proxy,
        // The dev-only "Break something" menu (web/src/components/shell/BreakSomethingMenu.tsx)
        // drives `devctl serve` (cmd/devctl/serve.go); see src/lib/devctlProxy.ts for the
        // prefix rewrite and the "not running" response.
        [DEVCTL_PROXY_KEY]: devctlProxyOptions(devctl),
      },
    },
    preview: { proxy },
  }
})
