# Piperoom web (Vite, React, TypeScript)

## Dev workflow

1. Start the Go server (dev auth mode, the default) on `127.0.0.1:8090`.
2. `npm run dev`, then open <http://localhost:5173>.

The browser talks to ONE origin. Vite proxies `/api` (with the room WebSocket) and `/auth` to the Go
server, so the HttpOnly `piperoom_session` cookie works over plain http. The client uses relative
paths only and never sends a user-id header. Under `npm run dev` a login screen / user switcher calls
`POST /auth/dev-login`; "Log out" calls `POST /auth/logout`.

- `PIPEROOM_API=http://127.0.0.1:9000 npm run dev` points the proxy at another server port.
- Keep Vite on `localhost` (or `127.0.0.1`): the proxy does not rewrite `Host`, and the server
  allows loopback names on any port (`421 bad_host` otherwise).
- `VITE_FIXTURES=1 npm run dev` runs the UI without a server (no login).
- `VITE_DOCS_BASE_URL` (build time, optional) is the URL prefix of the repo's docs, for example
  `https://github.com/<org>/piperoom/blob/main`. The read-only banner's "How to turn it on" link is
  `<prefix>/docs/pilot/quickstart.md`; without it the link is the bare repo path.
- On `401 session_expired` the server says its mode in `error.auth_mode` (`dev` shows the user picker,
  `oidc` goes to `/auth/login?return=...`). With no `auth_mode` the app asks `GET /api/me`, and if that does
  not say either it shows a plain "signed out" notice. It never redirects unless the mode is `oidc`.
  `VITE_AUTH_MODE=dev|oidc` is only a fallback for a server that sends no `auth_mode`. The app never
  redirects twice within 30 s, nor from an `/auth/` page; it shows a "Sign in" link instead.

---

This template provides a minimal setup to get React working in Vite with HMR and some ESLint rules.

Currently, two official plugins are available:

- [@vitejs/plugin-react](https://github.com/vitejs/vite-plugin-react/blob/main/packages/plugin-react) uses [Babel](https://babeljs.io/) for Fast Refresh
- [@vitejs/plugin-react-swc](https://github.com/vitejs/vite-plugin-react/blob/main/packages/plugin-react-swc) uses [SWC](https://swc.rs/) for Fast Refresh

## Expanding the ESLint configuration

If you are developing a production application, we recommend updating the configuration to enable type-aware lint rules:

```js
export default tseslint.config([
  globalIgnores(['dist']),
  {
    files: ['**/*.{ts,tsx}'],
    extends: [
      // Other configs...

      // Remove tseslint.configs.recommended and replace with this
      ...tseslint.configs.recommendedTypeChecked,
      // Alternatively, use this for stricter rules
      ...tseslint.configs.strictTypeChecked,
      // Optionally, add this for stylistic rules
      ...tseslint.configs.stylisticTypeChecked,

      // Other configs...
    ],
    languageOptions: {
      parserOptions: {
        project: ['./tsconfig.node.json', './tsconfig.app.json'],
        tsconfigRootDir: import.meta.dirname,
      },
      // other options...
    },
  },
])
```

You can also install [eslint-plugin-react-x](https://github.com/Rel1cx/eslint-react/tree/main/packages/plugins/eslint-plugin-react-x) and [eslint-plugin-react-dom](https://github.com/Rel1cx/eslint-react/tree/main/packages/plugins/eslint-plugin-react-dom) for React-specific lint rules:

```js
// eslint.config.js
import reactX from 'eslint-plugin-react-x'
import reactDom from 'eslint-plugin-react-dom'

export default tseslint.config([
  globalIgnores(['dist']),
  {
    files: ['**/*.{ts,tsx}'],
    extends: [
      // Other configs...
      // Enable lint rules for React
      reactX.configs['recommended-typescript'],
      // Enable lint rules for React DOM
      reactDom.configs.recommended,
    ],
    languageOptions: {
      parserOptions: {
        project: ['./tsconfig.node.json', './tsconfig.app.json'],
        tsconfigRootDir: import.meta.dirname,
      },
      // other options...
    },
  },
])
```
