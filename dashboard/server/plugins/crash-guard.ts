/**
 * Bun exits on an unhandled rejection. For a dashboard that is the wrong trade: one
 * forgotten `.catch` in a library - h3's event stream was one (see api/live.get.ts) -
 * took down every page for everyone, and the only trace was `error: undefined`. Log it,
 * with enough to find it, and keep serving.
 */
export default defineNitroPlugin(() => {
  process.on('unhandledRejection', (reason) => {
    console.error('unhandled rejection', reason instanceof Error ? reason : { reason })
  })
})
