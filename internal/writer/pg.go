package writer

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/amir20/drain/internal"
	_ "github.com/lib/pq"
	"go.uber.org/zap"
)

type PostgresWriter struct {
	channel chan internal.Event
	wg      *sync.WaitGroup
	logger  *zap.SugaredLogger
	db      *sql.DB
}

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
	return fmt.Sprintf("host=timescaledb user=%s password=%s dbname=drain sslmode=disable", user, pass), nil
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
		channel: make(chan internal.Event),
		wg:      &sync.WaitGroup{},
		logger:  logger,
		db:      db,
	}, nil
}

func (p *PostgresWriter) Start() chan internal.Event {
	p.wg.Go(func() {
		for event := range p.channel {
			jsonText, err := json.Marshal(event)
			if err != nil {
				p.logger.Errorf("failed to marshal event: %v", err)
				continue
			}
			_, err = p.db.Exec("INSERT INTO beacon (time, name, client_id, metadata) VALUES ($1, $2, $3, $4)", event.CreatedAt, event.Name, event.ServerID, jsonText)
			if err != nil {
				p.logger.Errorf("failed to insert event: %v", err)
			}
		}
	})
	return p.channel
}

func (p *PostgresWriter) Stop() {
	close(p.channel)
	p.wg.Wait()
	p.db.Close()
}
