/**
 * GitHub OAuth callback. Membership is an explicit allowlist of logins rather than
 * "any GitHub account", because these are product numbers for a real user base.
 */
export default defineOAuthGitHubEventHandler({
  config: {
    // `read:user` is enough to get the login; we never need repo or org scopes.
    scope: ['read:user'],
  },
  async onSuccess(event, { user }) {
    const allowed = allowedUsers()
    const login = String(user.login ?? '').toLowerCase()

    if (!allowed.includes(login)) {
      // Deliberately not "you are not on the list" with the list attached.
      await clearUserSession(event)
      return sendRedirect(event, '/login?error=forbidden')
    }

    await setUserSession(event, {
      user: {
        login: user.login,
        name: user.name ?? user.login,
        avatar: user.avatar_url,
      },
      loggedInAt: Date.now(),
    })

    return sendRedirect(event, '/')
  },
  onError(event, error) {
    console.error('github oauth failed:', error)
    return sendRedirect(event, '/login?error=oauth')
  },
})
