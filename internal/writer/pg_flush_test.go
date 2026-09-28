package writer

import (
	"database/sql"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/amir20/drain/internal"
	"go.uber.org/zap"
)

// Needs a real Postgres: DRAIN_TEST_DATABASE_URL=postgres://... go test ./internal/writer
// The table is created in a temporary schema, so any scratch database will do.
func testWriter(t *testing.T) *PostgresWriter {
	t.Helper()
	url := os.Getenv("DRAIN_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("DRAIN_TEST_DATABASE_URL not set")
	}
	db, err := sql.Open("postgres", url)
	if err != nil {
		t.Fatal(err)
	}
	// One connection, so the temporary schema is the one every statement sees.
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	if _, err := db.Exec(`CREATE TEMP TABLE beacon (
		time timestamptz NOT NULL, name text NOT NULL, client_id text NOT NULL, metadata jsonb)`); err != nil {
		t.Fatal(err)
	}
	return &PostgresWriter{db: db, logger: zap.NewNop().Sugar()}
}

func event(t *testing.T, name, client, raw string) internal.Event {
	t.Helper()
	var e internal.Event
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		t.Fatal(err)
	}
	e.Name, e.ServerID, e.CreatedAt, e.Raw = name, client, time.Now(), json.RawMessage(raw)
	return e
}

func TestFlushWritesBatch(t *testing.T) {
	p := testWriter(t)
	at := time.Date(2026, 9, 28, 12, 34, 56, 789000000, time.UTC)
	a := event(t, "events", "a", `{"browser":"x"}`)
	a.CreatedAt = at
	p.flush([]internal.Event{a, event(t, "start", "b", `{"version":"v1"}`)})

	var n int
	if err := p.db.QueryRow(`SELECT count(*) FROM beacon`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("rows = %d, want 2", n)
	}
	var got time.Time
	var browser string
	if err := p.db.QueryRow(`SELECT time, metadata ->> 'browser' FROM beacon WHERE client_id = 'a'`).Scan(&got, &browser); err != nil {
		t.Fatal(err)
	}
	if !got.Equal(at) || browser != "x" {
		t.Fatalf("row a = (%v, %q), want (%v, \"x\")", got, browser, at)
	}
}

// One beacon Postgres refuses must not take the rest of its batch down with it.
func TestFlushIsolatesBadRow(t *testing.T) {
	p := testWriter(t)
	p.flush([]internal.Event{
		event(t, "events", "good1", `{}`),
		event(t, "events", "bad", `{"browser":"a\u0000b"}`),
		event(t, "events", "good2", `{}`),
	})

	rows, err := p.db.Query(`SELECT client_id FROM beacon ORDER BY client_id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			t.Fatal(err)
		}
		got = append(got, c)
	}
	if len(got) != 2 || got[0] != "good1" || got[1] != "good2" {
		t.Fatalf("rows = %v, want [good1 good2]", got)
	}
}
