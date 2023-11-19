package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/amir20/drain/internal/web"
	"github.com/amir20/drain/internal/writer"
)

var (
	version = "head"
)

func main() {
	log.Printf("Starting drain version %s\n", version)
	writer := writer.NewParquetWriter()
	channel := writer.Start()
	srv := web.NewHTTPServer(channel)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Println("Accepting connections now on port 4000...")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen and serve returned err: %v", err)
		}
	}()
	<-ctx.Done()

	if err := srv.Shutdown(context.TODO()); err != nil {
		log.Printf("Server shutdown returned an err: %v\n", err)
	}
	writer.Stop()
}
