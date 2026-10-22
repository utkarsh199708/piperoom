import { EventEmitter } from 'node:events'
import { describe, expect, it } from 'vitest'
import type { ProxyOptions } from 'vite'
import { DEVCTL_PROXY_KEY, devctlLoopbackGuard, devctlProxyOptions } from './devctlProxy'

type Configure = NonNullable<ProxyOptions['configure']>

// The shape the proxy error handler writes to. http-proxy hands it a real
// ServerResponse; this recorder captures exactly the calls the handler makes.
function recordingResponse() {
  const out = {
    headersSent: false,
    status: 0,
    headers: {} as Record<string, string>,
    body: '',
    writeHead(status: number, headers: Record<string, string>) {
      out.status = status
      out.headers = headers
      return out
    },
    end(body?: string) {
      out.body = body ?? ''
      return out
    },
  }
  return out
}

function refusedConnection() {
  return Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:7071'), { code: 'ECONNREFUSED' })
}

// http-proxy emits 'error' on the server it passes to `configure`; a bare
// EventEmitter drives that wiring without opening a socket.
function configured(target = 'http://127.0.0.1:7071') {
  const options = devctlProxyOptions(target)
  const proxy = new EventEmitter()
  ;(options.configure as Configure)(proxy as unknown as Parameters<Configure>[0], options)
  return { options, proxy }
}

describe('devctl proxy', () => {
  it('answers a refused connection with 503 JSON devctl_not_running', () => {
    const { proxy } = configured()
    const res = recordingResponse()
    proxy.emit('error', refusedConnection(), { url: '/scenarios' }, res)
    expect(res.status).toBe(503)
    expect(res.headers['Content-Type']).toBe('application/json')
    expect(JSON.parse(res.body)).toEqual({ error: 'devctl_not_running' })
  })

  it('answers any other proxy error with 502 devctl_proxy_error', () => {
    const { proxy } = configured()
    const res = recordingResponse()
    proxy.emit('error', new Error('socket hang up'), { url: '/scenarios' }, res)
    expect(res.status).toBe(502)
    expect(JSON.parse(res.body)).toEqual({ error: 'devctl_proxy_error' })
  })

  it('leaves an already-started response alone', () => {
    const { proxy } = configured()
    const res = {
      headersSent: true,
      ended: false,
      writeHead() {
        throw new Error('must not write a second status line')
      },
      end() {
        res.ended = true
      },
    }
    proxy.emit('error', refusedConnection(), { url: '/scenarios' }, res)
    expect(res.ended).toBe(true)
  })

  it('proxies /devctl at the devctl root (prefix stripped, loopback Host)', () => {
    const { options } = configured()
    expect(options.target).toBe('http://127.0.0.1:7071')
    expect(options.changeOrigin).toBe(true)
    expect(options.rewrite?.('/devctl/scenarios')).toBe('/scenarios')
    expect(options.rewrite?.('/devctl/break/schema-change')).toBe('/break/schema-change')
  })
})

describe('devctl loopback guard', () => {
  type GuardConfig = { server: { host?: string | boolean; proxy: Record<string, unknown> } }

  function apply(host: string | boolean) {
    const config: GuardConfig = { server: { host, proxy: { [DEVCTL_PROXY_KEY]: {} } } }
    const hook = devctlLoopbackGuard().config as unknown as (c: GuardConfig) => void
    hook(config)
    return config
  }

  it('keeps the proxy when Vite binds to loopback', () => {
    expect(apply('localhost').server.proxy[DEVCTL_PROXY_KEY]).toBeDefined()
    expect(apply('127.0.0.1').server.proxy[DEVCTL_PROXY_KEY]).toBeDefined()
  })

  it('removes the proxy when Vite binds beyond loopback', () => {
    expect(apply('0.0.0.0').server.proxy[DEVCTL_PROXY_KEY]).toBeUndefined()
    expect(apply(true).server.proxy[DEVCTL_PROXY_KEY]).toBeUndefined()
  })
})
