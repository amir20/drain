package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/amir20/drain/internal"
	"github.com/amir20/drain/internal/cleanup"
	"github.com/amir20/drain/internal/web"
	"github.com/amir20/drain/internal/writer"
	"go.uber.org/zap"
)

var (
	version = "head"
)

var dev = flag.Bool("dev", false, "enables dev mode")
var clean = flag.Bool("clean-only", false, "only clean the data directory")

func main() {
	flag.Parse()
	logger := zap.Must(zap.NewProduction())
	if *dev {
		logger = zap.Must(zap.NewDevelopment(zap.IncreaseLevel(zap.DebugLevel)))
	}
	defer logger.Sync()
	sugar := logger.Sugar()

	if *clean {
		sugar.Info("Cleaning data directory")
		if err := cleanup.Cleanup(sugar); err != nil {
			sugar.Fatal(err)
		}
		return
	}

	sugar.Infof("Starting drain %s", version)

	if _, err := os.Stat("./data"); os.IsNotExist(err) {
		sugar.Info("Creating data directory")
		if err := os.Mkdir("./data", 0755); err != nil {
			sugar.Fatal(err)
		}
	}

	daily := time.Tick(24 * time.Hour)
	go func() {
		sugar.Infof("Starting cleanup routine")
		for day := range daily {
			sugar.Infof("Cleaning data directory at %s", day)
			if err := cleanup.Cleanup(sugar); err != nil {
				sugar.Error(err)
			}
		}
	}()

	writer := writer.NewParquetWriter(sugar)
	channel := writer.Start()

	events := sendToAllChannels(channel)
	srv := web.NewHTTPServer(events, sugar)

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

func sendToAllChannels(channels ...chan internal.Event) chan internal.Event {
	out := make(chan internal.Event)
	go func() {
		for event := range out {
			for _, channel := range channels {
				channel <- event
			}
		}
	}()
	return out
}
