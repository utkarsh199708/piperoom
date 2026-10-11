// Build-time flags. Vite statically replaces `import.meta.env.DEV` at build time
// (true in dev/test, false in the production bundle), so hoisting it into a small
// module keeps dev-only rendering in one place and lets tests override it.
export const isDev = import.meta.env.DEV
