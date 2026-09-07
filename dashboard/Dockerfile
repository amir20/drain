# syntax=docker/dockerfile:1

# Pinned to the BUILD platform, like the Go image does. Nitro's output is plain
# JavaScript and nothing here has a native dependency, so there is no reason to build it
# once per target architecture - and doing so ran the arm64 pass under QEMU on an amd64
# runner: `bun install` took 45s and `bun run build` 279s, 324s of a 392s build, while
# the Go image cross-compiled four platforms in 73s.
FROM --platform=$BUILDPLATFORM oven/bun:1-alpine AS build
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile
COPY . .
RUN bun run build

FROM oven/bun:1-alpine AS runtime
WORKDIR /app
ENV NODE_ENV=production NITRO_PORT=3000 NITRO_HOST=0.0.0.0
COPY --from=build /app/.output ./.output
# Expands /run/secrets/* into the environment Nuxt reads. See docker-entrypoint.sh.
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
EXPOSE 3000
USER bun
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["bun", ".output/server/index.mjs"]
