package writer

import (
	"database/sql"
	"encoding/json"
	"fmt"
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

func NewPostgresWriter(logger *zap.SugaredLogger, user, pass string) (*PostgresWriter, error) {
	dsn := fmt.Sprintf("host=timescaledb user=%s password=%s dbname=drain sslmode=disable", user, pass)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("error opening database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("error pinging database: %w", err)
	}

	return &PostgresWriter{
		channel: make(chan internal.Event),
		wg:      &sync.WaitGroup{},
		logger:  logger,
		db:      db,
	}, nil
}

func (p *PostgresWriter) Start() chan internal.Event {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
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
	}()
	return p.channel
}

func (p *PostgresWriter) Stop() {
	close(p.channel)
	p.wg.Wait()
	p.db.Close()
}
