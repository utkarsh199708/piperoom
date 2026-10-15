import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'
import { fileURLToPath, URL } from 'node:url'

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  // Where the Go server listens. Override with PIPEROOM_API, for example
  // `PIPEROOM_API=http://127.0.0.1:9000 npm run dev`.
  const env = loadEnv(mode, process.cwd(), '')
  const api = process.env.PIPEROOM_API ?? env.PIPEROOM_API ?? 'http://127.0.0.1:8090'

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
    plugins: [react()],
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
    server: { proxy },
    preview: { proxy },
  }
})
