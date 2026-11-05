import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import '@fontsource-variable/geist'
import '@fontsource-variable/geist-mono'
import './index.css'
import App from './App.tsx'
import { restoreReturnHash } from './lib/auth'
import { landingHash } from './lib/fleet'

// Back from /auth/login: put the #/rooms/... part of the URL back (once).
restoreReturnHash()

// X5b: the app opened without a route lands on the fleet. Replace, so Back does not bounce to the bare URL.
const landing = landingHash(window.location.hash)
if (landing) window.history.replaceState(window.history.state, '', landing)

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
