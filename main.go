package main

import (
	"context"
	"errors"
	"flag"

	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/amir20/drain/internal"
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
	logger := zap.Must(zap.NewProduction())
	if *dev {
		logger = zap.Must(zap.NewDevelopment(zap.IncreaseLevel(zap.DebugLevel)))
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

	parquetWriter := writer.NewParquetWriter(sugar)
	influxWriter := writer.NewInfluxWriter(sugar)

	events := sendToAllChannels(parquetWriter.Start(), influxWriter.Start())
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
	parquetWriter.Stop()
}

func sendToAllChannels(channels ...chan<- internal.Event) chan internal.Event {
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
