export default defineNuxtConfig({
  compatibilityDate: '2025-07-15',
  modules: ['nuxt-auth-utils'],
  devtools: { enabled: false },
  css: ['~/assets/app.css'],
  ssr: true,
  nitro: {
    preset: 'bun',
    // The dashboard is a handful of aggregate reads; no need to bundle a cache layer.
    minify: true,
  },
  runtimeConfig: {
    // Without an explicit maxAge, h3 seals the session cookie with ttl 0 and sets no
    // expiry, so it is valid forever. A week means a lost laptop stops being an open
    // door on its own; the allowlist re-check in server/middleware/require-auth.ts
    // handles revocation before then.
    session: {
      maxAge: 60 * 60 * 24 * 7,
      cookie: { sameSite: 'lax' },
    },
    databaseUrl: '',
    // Comma-separated GitHub logins allowed past the OAuth callback. Empty means
    // nobody gets in, which is the safe default if the variable is ever unset.
    githubAllowedUsers: '',
    public: {
      appName: 'Dozzle Analytics',
    },
  },
  app: {
    head: {
      title: 'Dozzle Analytics',
      meta: [{ name: 'viewport', content: 'width=device-width, initial-scale=1' }],
      link: [{ rel: 'icon', href: '/favicon.svg' }],
    },
  },
  typescript: { strict: true },
})
