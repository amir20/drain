// Package migrate applies the SQL files under migrations/ to the beacon database.
//
// The production database predates these migrations, so init/01_init.sql is never
// re-run there: anything added after the first deploy has to arrive this way. Files are
// applied in filename order and recorded in schema_migrations by checksum, so editing a
// migration re-applies it (every file is written to be idempotent) while an unchanged
// one is skipped.
package migrate

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io/fs"
	"sort"

	"go.uber.org/zap"
)

const createTable = `
CREATE TABLE IF NOT EXISTS schema_migrations (
	filename   text PRIMARY KEY,
	checksum   text NOT NULL,
	applied_at timestamptz NOT NULL DEFAULT now()
)`

// Run applies every *.sql file in fsys (rooted at dir) that is new or has changed.
func Run(db *sql.DB, fsys fs.FS, dir string, logger *zap.SugaredLogger) error {
	if _, err := db.Exec(createTable); err != nil {
		return fmt.Errorf("creating schema_migrations: %w", err)
	}

	entries, err := fs.ReadDir(fsys, dir)
	if err != nil {
		return fmt.Errorf("reading %s: %w", dir, err)
	}

	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && len(e.Name()) > 4 && e.Name()[len(e.Name())-4:] == ".sql" {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	for _, name := range names {
		body, err := fs.ReadFile(fsys, dir+"/"+name)
		if err != nil {
			return fmt.Errorf("reading %s: %w", name, err)
		}
		sum := sha256.Sum256(body)
		checksum := hex.EncodeToString(sum[:])

		var existing string
		err = db.QueryRow("SELECT checksum FROM schema_migrations WHERE filename = $1", name).Scan(&existing)
		switch {
		case err == sql.ErrNoRows:
			// new migration
		case err != nil:
			return fmt.Errorf("checking %s: %w", name, err)
		case existing == checksum:
			logger.Debugf("migration %s already applied", name)
			continue
		default:
			logger.Infof("migration %s changed, re-applying", name)
		}

		logger.Infof("applying migration %s", name)
		// Statement at a time: TimescaleDB rejects CREATE MATERIALIZED VIEW ... WITH
		// (timescaledb.continuous) inside a transaction block, so the file cannot be
		// wrapped in one.
		for i, stmt := range Split(string(body)) {
			if _, err := db.Exec(stmt); err != nil {
				return fmt.Errorf("%s statement %d: %w", name, i+1, err)
			}
		}

		if _, err := db.Exec(
			`INSERT INTO schema_migrations (filename, checksum, applied_at)
			 VALUES ($1, $2, now())
			 ON CONFLICT (filename) DO UPDATE SET checksum = EXCLUDED.checksum, applied_at = now()`,
			name, checksum); err != nil {
			return fmt.Errorf("recording %s: %w", name, err)
		}
	}

	return nil
}
