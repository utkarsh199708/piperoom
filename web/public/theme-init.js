// Sets <html data-theme> before the first paint, so there is no flash of the
// wrong theme. It is a file, not an inline script, because the server's CSP is
// script-src 'self' (internal/httpapi/static.go).
// Keep the storage key and the values in sync with src/lib/theme.ts.
;(function () {
  var theme = 'dark'
  var saved = null
  try {
    saved = window.localStorage.getItem('piperoom-theme')
  } catch (e) {
    // storage blocked: fall through to the OS setting
  }
  if (saved === 'light' || saved === 'dark') {
    theme = saved
  } else {
    try {
      if (window.matchMedia('(prefers-color-scheme: light)').matches) theme = 'light'
    } catch (e) {
      // keep dark
    }
  }
  var root = document.documentElement
  root.setAttribute('data-theme', theme)
  root.style.colorScheme = theme
})()
