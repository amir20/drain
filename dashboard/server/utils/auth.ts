/**
 * Logins permitted into the dashboard, lower-cased. An unset or empty
 * NUXT_GITHUB_ALLOWED_USERS means nobody, not everybody.
 */
export function allowedUsers(): string[] {
  return useRuntimeConfig()
    .githubAllowedUsers.split(',')
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean)
}
