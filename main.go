package main

import (
	"context"
	"embed"
	"errors"
	"flag"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/amir20/drain/internal/migrate"
	"github.com/amir20/drain/internal/web"
	"github.com/amir20/drain/internal/writer"
	"go.uber.org/zap"
)

// The production database predates these files and init/01_init.sql only runs on an
// empty volume, so schema changes ship with the binary and are applied by -migrate.
//
//go:embed migrations/*.sql
var migrations embed.FS

var (
	version = "head"
)

var dev = flag.Bool("dev", false, "enables dev mode")
var migrateOnly = flag.Bool("migrate", false, "apply pending database migrations and exit")

func main() {
	flag.Parse()
	logger := zap.Must(zap.NewProduction())
	if *dev {
		logger = zap.Must(zap.NewDevelopment(zap.IncreaseLevel(zap.DebugLevel)))
	}
	defer logger.Sync()
	sugar := logger.Sugar()

	if *migrateOnly {
		db, err := writer.Connect("postgres", "password")
		if err != nil {
			sugar.Fatal(err)
		}
		defer db.Close()
		if err := migrate.Run(db, migrations, "migrations", sugar); err != nil {
			sugar.Fatal(err)
		}
		sugar.Info("Migrations up to date")
		return
	}

	sugar.Infof("Starting drain %s", version)

	pgWriter, err := writer.NewPostgresWriter(sugar, "postgres", "password")
	if err != nil {
		sugar.Fatalf("failed to create writer: %v", err)
	}

	ips, err := web.NewIPHasherFromEnv()
	if err != nil {
		sugar.Fatal(err)
	}
	if !ips.Keyed() {
		sugar.Warn("DRAIN_IP_HASH_KEY is not set: client IPs are stored as plain SHA-256, which is reversible by brute force")
	}

	srv := web.NewHTTPServer(pgWriter.Start(), ips, sugar)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		sugar.Infof("Listening on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			sugar.Fatalf("server listen returned err: %v", err)
		}
	}()
	<-ctx.Done()

	// Bounded so a stuck handler cannot hold the container past Swarm's own stop timeout;
	// pgWriter.Stop still runs and flushes whatever was accepted.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		sugar.Errorf("server shutdown returned err: %v", err)
	}
	pgWriter.Stop()
}
