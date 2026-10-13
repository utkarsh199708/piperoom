import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'

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
    '/api': { target: api, ws: true },
    '/auth': { target: api },
  }

  return {
    plugins: [react()],
    server: { proxy },
    preview: { proxy },
  }
})
