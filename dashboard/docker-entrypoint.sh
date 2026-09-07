#!/bin/sh
# Bridges Docker secrets to the environment.
#
# Swarm delivers a secret as a file under /run/secrets, but Nuxt's runtimeConfig and
# nuxt-auth-utils both read plain environment variables, and neither supports a _FILE
# convention. So expand it here, before the server starts.
#
# The drain image cannot do this - it is FROM scratch and has no shell - so it reads its
# secret in Go instead. See internal/writer/pg.go.
set -eu

# Every FOO_FILE becomes FOO. Nothing is special-cased, so a new secret needs only a
# compose entry.
for file_var in $(env | sed -n 's/^\([A-Za-z_][A-Za-z0-9_]*\)_FILE=.*/\1_FILE/p'); do
  var="${file_var%_FILE}"
  eval "path=\${$file_var}"
  [ -n "$path" ] || continue
  if [ ! -r "$path" ]; then
    echo "entrypoint: $file_var points at $path, which is not readable" >&2
    exit 1
  fi
  # Trailing newlines are what editors and `echo` leave behind; a password carrying one
  # fails authentication in a way that reads like a wrong password.
  value=$(sed -e 's/[[:space:]]*$//' "$path" | tr -d '\n')
  if [ -z "$value" ]; then
    echo "entrypoint: $path is empty" >&2
    exit 1
  fi
  export "$var=$value"
  unset "$file_var"
done

# The dashboard wants one connection string, but the password is its own secret so that
# there is a single copy of it rather than one here and one in the database's. Compose the
# URL unless a complete one was supplied outright, which is how dev runs.
if [ -z "${NUXT_DATABASE_URL:-}" ]; then
  # Percent-encoded, so a password holding `@`, `/`, `+` or `=` - anything a base64
  # string might - cannot be read as part of the host or path. Every other consumer of
  # this secret takes the raw bytes; this is the only place that needs encoding.
  pw=$(PW="${POSTGRES_PASSWORD:-password}" bun -e 'process.stdout.write(encodeURIComponent(process.env.PW))')
  export NUXT_DATABASE_URL="postgres://${POSTGRES_USER:-postgres}:${pw}@timescaledb:5432/${POSTGRES_DB:-drain}"
fi

exec "$@"
