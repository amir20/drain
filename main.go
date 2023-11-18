package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/amir20/drain/internal/writer"
)

type BeaconEvent struct {
	Version           string `json:"version"`
	Browser           string `json:"browser"`
	AuthProvider      string `json:"authProvider"`
	HasDocumentation  bool   `json:"hasDocumentation"`
	FilterLength      int    `json:"filterLength"`
	RemoteHostLength  int    `json:"remoteHostLength"`
	HasCustomAddress  bool   `json:"hasCustomAddress"`
	HasCustomBase     bool   `json:"hasCustomBase"`
	HasHostname       bool   `json:"hasHostname"`
	RunningContainers int    `json:"runningContainers"`
}

func dataCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var b BeaconEvent

	err := json.NewDecoder(r.Body).Decode(&b)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	row := writer.WriterRow{
		Browser:           b.Browser,
		AuthProvider:      b.AuthProvider,
		RemoteHostLength:  b.RemoteHostLength,
		HasDocumentation:  b.HasDocumentation,
		FilterLength:      b.FilterLength,
		Version:           b.Version,
		HasCustomAddress:  b.HasCustomAddress,
		HasCustomBase:     b.HasCustomBase,
		HasHostname:       b.HasHostname,
		RunningContainers: b.RunningContainers,
	}

	row.RemoteIP = r.Header.Get("X-Forwarded-For")

	channel <- row

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("Created"))
}

var channel chan writer.WriterRow

func main() {
	writer := writer.NewParquetWriter()
	channel = writer.Start()

	mux := http.NewServeMux()
	mux.HandleFunc("/data", dataCreate)
	srv := &http.Server{
		Addr:    ":4000",
		Handler: mux,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Println("Starting server")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen and serve returned err: %v", err)
		}
	}()
	<-ctx.Done()

	log.Println("got interruption signal")
	if err := srv.Shutdown(context.TODO()); err != nil {
		log.Printf("server shutdown returned an err: %v\n", err)
	}
	log.Println("Waiting")
	writer.Stop()
	log.Println("Done")
}
