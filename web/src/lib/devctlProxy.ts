import type { Plugin, ProxyOptions } from 'vite'
import type { ServerResponse } from 'node:http'
import type { Socket } from 'node:net'
import { isLoopbackHost } from './loopback'

// The Vite dev server proxies /devctl to `devctl serve` (cmd/devctl/serve.go), an
// unauthenticated, loopback-only break/fix API, so the dev-only "Break something"
// menu can call it same-origin. The proxy below uses `changeOrigin: true` so the
// proxied Host is 127.0.0.1:7071, which is what lets it pass devctl's loopback
// allow-list — but that also means that if Vite is bound beyond loopback
// (`vite --host`), anyone on the network reaches that API and the Host/Origin
// checks are satisfied. devctl's loopback boundary must not depend on Vite's bind
// address, so this plugin removes the /devctl proxy whenever the resolved server
// host is not loopback, unless the developer explicitly opts in with
// PIPEROOM_DEVCTL_ALLOW_REMOTE=1.
export const DEVCTL_PROXY_KEY = '^/devctl(/|$)'

export const devctlLoopbackGuard = (): Plugin => ({
  name: 'piperoom:devctl-loopback-guard',
  config(config) {
    if (isLoopbackHost(config.server?.host)) return
    if (process.env.PIPEROOM_DEVCTL_ALLOW_REMOTE === '1') return
    const proxy = config.server?.proxy
    if (proxy) delete proxy[DEVCTL_PROXY_KEY]
  },
})

// net errors that mean nothing is listening at the target address (devctl serve
// was never started, or just died). http-proxy reports the underlying error on
// its `error` event, with the syscall code preserved.
const NOT_RUNNING_CODES = new Set(['ECONNREFUSED', 'ECONNRESET', 'EHOSTUNREACH', 'ENOTFOUND'])

type ProxyError = Error & { code?: string }

// True when the failure is "devctl serve is not running" rather than an error
// from a devctl that is up. The menu uses the same marker on the client side.
export function isDevctlNotRunning(err: ProxyError): boolean {
  return err.code !== undefined && NOT_RUNNING_CODES.has(err.code)
}

// Answer a proxy failure with a small JSON body. Without this Vite hands the
// browser an empty 500, which the menu cannot tell apart from a real 500, and a
// developer has no clue that the proxy target is simply absent. `res` is a raw
// socket when the failure happens before a response exists; there is nothing
// safe to write to it.
export function writeProxyError(res: ServerResponse | Socket, status: number, error: string): void {
  if (typeof (res as ServerResponse).writeHead !== 'function') return
  const out = res as ServerResponse
  if (out.headersSent) {
    out.end()
    return
  }
  const body = JSON.stringify({ error })
  out.writeHead(status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' })
  out.end(body)
}

// The server.proxy entry for the dev-only "Break something" menu. devctl serves
// at the ROOT (/scenarios, /break/..., /fix/...), so the /devctl prefix is
// stripped here. changeOrigin is on so devctl sees Host 127.0.0.1:7071: its Host
// allow-list is port-exact (unlike the Go server, which accepts any loopback
// port). Deliberately absent from preview.proxy: preview serves the production
// bundle, which has no menu.
export function devctlProxyOptions(target: string): ProxyOptions {
  return {
    target,
    changeOrigin: true,
    rewrite: (path) => path.replace(/^\/devctl/, ''),
    configure: (proxy) => {
      proxy.on('error', (err: ProxyError, _req: unknown, res: ServerResponse | Socket) => {
        if (isDevctlNotRunning(err)) writeProxyError(res, 503, 'devctl_not_running')
        else writeProxyError(res, 502, 'devctl_proxy_error')
      })
    },
  }
}
