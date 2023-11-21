package main

import (
	"context"
	"errors"
	"flag"

	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/amir20/drain/internal/web"
	"github.com/amir20/drain/internal/writer"
	"go.uber.org/zap"
)

var (
	version = "head"
)

var dev = flag.Bool("dev", false, "enables dev mode")

func main() {
	flag.Parse()
	logger, _ := zap.NewProduction()
	if *dev {
		logger, _ = zap.NewDevelopment()
	}
	defer logger.Sync()
	sugar := logger.Sugar()

	sugar.Infof("Starting drain %s", version)

	if _, err := os.Stat("./data"); os.IsNotExist(err) {
		sugar.Info("Creating data directory")
		if err := os.Mkdir("./data", 0755); err != nil {
			sugar.Fatal(err)
		}
	}

	writer := writer.NewParquetWriter(sugar)
	channel := writer.Start()
	srv := web.NewHTTPServer(channel, sugar)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		sugar.Infof("Listening on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			sugar.Fatalf("server listen returned err: %w", err)
		}
	}()
	<-ctx.Done()

	if err := srv.Shutdown(context.TODO()); err != nil {
		sugar.Fatalf("server shutdown returned err: %w", err)
	}
	writer.Stop()
}
