package writer

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
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
func DSN(user, pass string) string {
	if url, ok := os.LookupEnv("DATABASE_URL"); ok && url != "" {
		return url
	}
	return fmt.Sprintf("host=timescaledb user=%s password=%s dbname=drain sslmode=disable", user, pass)
}

// Connect opens the beacon database and verifies it is reachable.
func Connect(user, pass string) (*sql.DB, error) {
	db, err := sql.Open("postgres", DSN(user, pass))
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
