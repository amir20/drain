/**
 * Server-sent events for the Live page. Each message is one LiveTick (server/utils/live.ts).
 *
 * Auth runs once, when require-auth.ts sees the request open. A stream that stayed open
 * for days would outlive both the session's week and removal from the allowlist, so it
 * is closed after MAX_AGE_MS; EventSource reconnects on its own, and the reconnect goes
 * through the middleware again like any other request.
 */
const MAX_AGE_MS = 15 * 60_000

export default defineEventHandler(async (event) => {
  // Reverse proxies buffer responses by default, which would deliver ticks in clumps.
  setResponseHeader(event, 'X-Accel-Buffering', 'no')

  const stream = createEventStream(event)
  const unsubscribe = subscribeLive((tick) => {
    void stream.push({ event: 'tick', data: JSON.stringify(tick) })
  })
  const expiry = setTimeout(() => void stream.close(), MAX_AGE_MS)

  stream.onClosed(async () => {
    clearTimeout(expiry)
    unsubscribe()
    await stream.close()
  })

  return stream.send()
})
