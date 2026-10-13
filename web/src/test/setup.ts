import { afterEach } from 'vitest'
import { cleanup, configure } from '@testing-library/react'
import '@testing-library/jest-dom/vitest'

// findBy*/waitFor wait up to 1 s by default. Under a full parallel run the lazy chunk of the
// command menu (and Radix dialogs) can take longer than that on a busy machine, which made
// AppShell.test flaky. A longer wait changes no assertion; a passing test still returns at once.
configure({ asyncUtilTimeout: 5000 })

afterEach(() => {
  cleanup()
})
