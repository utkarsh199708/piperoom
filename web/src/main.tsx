import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'
import { restoreReturnHash } from './lib/auth'

// Back from /auth/login: put the #/rooms/... part of the URL back (once).
restoreReturnHash()

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
