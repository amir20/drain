package writer

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/amir20/drain/internal"
	"github.com/lib/pq"
	"go.uber.org/zap"
)

type PostgresWriter struct {
	channel chan internal.Event
	wg      *sync.WaitGroup
	logger  *zap.SugaredLogger
	db      *sql.DB
}

const (
	// queueSize is how many beacons the handlers can hand off before they start waiting on
	// the database. It absorbs a slow flush without letting a stalled database grow memory
	// without bound: once it fills, requests block, which is the backpressure we want.
	queueSize = 4096
	// maxBatch and flushEvery bound a batch by size and by age. A beacon is visible in the
	// database at most flushEvery after it arrived, far inside the hourly refresh.
	maxBatch   = 500
	flushEvery = 250 * time.Millisecond
)

// DSN is the connection string for the beacon database. DATABASE_URL wins when set so
// the same binary can run migrations from a one-shot container or against a local
// Postgres; otherwise it falls back to the in-stack service name.
//
// The password comes from POSTGRES_PASSWORD_FILE (a Docker secret, in production) or
// POSTGRES_PASSWORD (docker-compose.override.yml, in dev) - the same two variables the
// Postgres image itself reads, so one value configures both ends. This image is FROM
// scratch and has no shell, so there is no entrypoint that could expand the file into
// the environment the way the dashboard's does - it has to be read here.
func DSN(user, pass string) (string, error) {
	if url, ok := os.LookupEnv("DATABASE_URL"); ok && url != "" {
		return url, nil
	}
	// A misconfigured secret must not fall through to the caller's default: that default
	// is the historical password, so a silent fallback would connect anyway and hide the
	// breakage until the day the password actually differs.
	if path, ok := os.LookupEnv("POSTGRES_PASSWORD_FILE"); ok && path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("reading POSTGRES_PASSWORD_FILE %s: %w", path, err)
		}
		// Trailing newlines are what every editor and `echo` leave behind, and Postgres
		// would treat one as part of the password.
		if pass = strings.TrimRight(string(b), "\r\n"); pass == "" {
			return "", fmt.Errorf("POSTGRES_PASSWORD_FILE %s is empty", path)
		}
	} else if p := os.Getenv("POSTGRES_PASSWORD"); p != "" {
		pass = p
	}
	// A URL rather than key=value: lib/pq ends an unquoted value at whitespace, so a
	// password holding a space or a quote would otherwise be cut short. url.UserPassword
	// escapes anything, matching the dashboard's encodeURIComponent.
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(user, pass),
		Host:     "timescaledb",
		Path:     "/drain",
		RawQuery: "sslmode=disable",
	}
	return u.String(), nil
}

// Connect opens the beacon database and verifies it is reachable.
func Connect(user, pass string) (*sql.DB, error) {
	dsn, err := DSN(user, pass)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("error opening database: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("error pinging database: %w", err)
	}
	return db, nil
}

func NewPostgresWriter(logger *zap.SugaredLogger, user, pass string) (*PostgresWriter, error) {
	db, err := Connect(user, pass)
	if err != nil {
		return nil, err
	}

	return &PostgresWriter{
		channel: make(chan internal.Event, queueSize),
		wg:      &sync.WaitGroup{},
		logger:  logger,
		db:      db,
	}, nil
}

func (p *PostgresWriter) Start() chan internal.Event {
	p.wg.Go(func() { batch(p.channel, maxBatch, flushEvery, p.flush) })
	return p.channel
}

// batch collects events into groups of at most size, handing each to flush when it is
// full or every interval, whichever comes first. It returns once in is closed and the
// last partial batch has been flushed.
func batch(in <-chan internal.Event, size int, interval time.Duration, flush func([]internal.Event)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	pending := make([]internal.Event, 0, size)
	send := func() {
		if len(pending) > 0 {
			flush(pending)
			pending = make([]internal.Event, 0, size)
		}
	}
	for {
		select {
		case e, ok := <-in:
			if !ok {
				send()
				return
			}
			pending = append(pending, e)
			if len(pending) >= size {
				send()
			}
		case <-ticker.C:
			send()
		}
	}
}

const insertBatch = `INSERT INTO beacon (time, name, client_id, metadata)
SELECT * FROM unnest($1::timestamptz[], $2::text[], $3::text[], $4::jsonb[])`

// flush writes a batch in one statement. A statement is all or nothing, and a single
// row Postgres refuses - a \u0000 in a string is valid JSON but not valid jsonb - would
// take every other install's beacon down with it, so a failed batch is retried row by
// row and only the bad rows are lost.
func (p *PostgresWriter) flush(events []internal.Event) {
	times := make([]time.Time, 0, len(events))
	names := make([]string, 0, len(events))
	clients := make([]string, 0, len(events))
	metadata := make([]string, 0, len(events))
	for _, e := range events {
		m, err := e.Metadata()
		if err != nil {
			p.logger.Errorf("failed to marshal event: %v", err)
			continue
		}
		times = append(times, e.CreatedAt)
		names = append(names, e.Name)
		clients = append(clients, e.ServerID)
		metadata = append(metadata, string(m))
	}
	if len(times) == 0 {
		return
	}

	_, err := p.db.Exec(insertBatch, pq.Array(times), pq.Array(names), pq.Array(clients), pq.Array(metadata))
	if err == nil {
		return
	}
	if len(times) == 1 {
		p.logger.Errorf("failed to insert event: %v", err)
		return
	}
	p.logger.Warnf("batch of %d failed, retrying one at a time: %v", len(times), err)
	for i := range times {
		if _, err := p.db.Exec(
			"INSERT INTO beacon (time, name, client_id, metadata) VALUES ($1, $2, $3, $4)",
			times[i], names[i], clients[i], metadata[i]); err != nil {
			p.logger.Errorf("failed to insert event: %v", err)
		}
	}
}

func (p *PostgresWriter) Stop() {
	close(p.channel)
	p.wg.Wait()
	p.db.Close()
}
