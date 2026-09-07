/**
 * Everything under /api is private, with two exceptions:
 *
 *   /api/health   - the container health probe, which runs without a session.
 *   /api/_auth/*  - nuxt-auth-utils' own session endpoints. The module registers
 *                   GET and DELETE handlers there; 401ing them breaks sign-out, because
 *                   clearing the session is exactly the thing you do when you no longer
 *                   want one.
 *
 * The allowlist is re-checked on every request, not just at login. The session cookie
 * carries the login it was issued to, so a cookie minted for someone since removed from
 * NUXT_GITHUB_ALLOWED_USERS would otherwise keep working until it expired.
 */
const PUBLIC = ['/api/health']
const PUBLIC_PREFIXES = ['/api/_auth/']

export default defineEventHandler(async (event) => {
  // Normalise before matching so a trailing slash cannot slip past the allowlist or the
  // guard itself.
  const raw = event.path.split('?')[0] ?? ''
  const path = raw.length > 1 ? raw.replace(/\/+$/, '') : raw

  if (path !== '/api' && !path.startsWith('/api/')) return
  if (PUBLIC.includes(path)) return
  if (PUBLIC_PREFIXES.some((p) => path.startsWith(p))) return

  const session = await getUserSession(event)
  const login = String(session?.user?.login ?? '').toLowerCase()

  if (!login) {
    throw createError({ statusCode: 401, statusMessage: 'Not signed in' })
  }

  if (!allowedUsers().includes(login)) {
    await clearUserSession(event)
    throw createError({ statusCode: 403, statusMessage: 'Not allowed' })
  }
})
