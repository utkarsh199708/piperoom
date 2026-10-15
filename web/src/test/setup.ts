import { afterEach } from 'vitest'
import { cleanup } from '@testing-library/react'
import '@testing-library/jest-dom/vitest'

// Storage outlives a test. SteerBox writes its draft 300 ms after typing, so on a slow run a
// draft from one test landed in localStorage and the next test mounted with text in the field.
afterEach(() => {
  cleanup()
  localStorage.clear()
  sessionStorage.clear()
})
