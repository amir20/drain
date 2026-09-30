/**
 * Server-sent events for the Live page. Each message is one LiveMessage
 * (server/utils/live.ts), sent as `event: <type>`.
 *
 * This deliberately does not use h3's createEventStream. As of h3 1.15 it hangs `.then`
 * off its writer's `closed` promise with no rejection handler, and when a browser leaves,
 * Bun cancels the response body, that promise rejects, and the unhandled rejection kills
 * the process - every navigation away from Live restarted the dashboard. A plain
 * ReadableStream gets `cancel()` instead, which is exactly the signal to unsubscribe on.
 *
 * Auth runs once, when require-auth.ts sees the request open. A stream that stayed open
 * for days would outlive both the session's week and removal from the allowlist, so it
 * is closed after MAX_AGE_MS; EventSource reconnects on its own, and the reconnect goes
 * through the middleware again like any other request.
 */
const MAX_AGE_MS = 15 * 60_000
/** A comment line now and then, so an idle proxy does not decide the stream is dead. */
const PING_MS = 20_000

export default defineEventHandler((event) => {
  setResponseHeaders(event, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    Connection: 'keep-alive',
    // Reverse proxies buffer responses by default, which would deliver beacons in clumps.
    'X-Accel-Buffering': 'no',
  })

  const encoder = new TextEncoder()
  let close = () => {}

  return new ReadableStream<Uint8Array>({
    start(controller) {
      let open = true
      let unsubscribe = () => {}
      const write = (chunk: string) => {
        if (!open) return
        try {
          controller.enqueue(encoder.encode(chunk))
        } catch {
          close()
        }
      }
      close = () => {
        if (!open) return
        open = false
        clearTimeout(expiry)
        clearInterval(ping)
        unsubscribe()
        try {
          controller.close()
        } catch {
          // Already cancelled by the client.
        }
      }
      const expiry = setTimeout(() => close(), MAX_AGE_MS)
      const ping = setInterval(() => write(': ping\n\n'), PING_MS)
      unsubscribe = subscribeLive((msg) => write(`event: ${msg.type}\ndata: ${JSON.stringify(msg)}\n\n`))
    },
    cancel() {
      close()
    },
  })
})
