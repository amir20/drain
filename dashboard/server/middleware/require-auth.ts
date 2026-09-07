/**
 * Everything under /api is private. Auth routes and the health probe are not, so the
 * OAuth dance can complete and the container can be health-checked without a session.
 */
const PUBLIC = ['/api/health']

export default defineEventHandler(async (event) => {
  const path = event.path.split('?')[0] ?? ''
  if (!path.startsWith('/api/')) return
  if (PUBLIC.includes(path)) return

  const session = await getUserSession(event)
  if (!session?.user) {
    throw createError({ statusCode: 401, statusMessage: 'Not signed in' })
  }
})
