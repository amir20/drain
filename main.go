package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/parquet-go/parquet-go"
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

type RowType struct {
	Browser           string
	AuthProvider      string
	RemoteIP          string
	Version           string
	RemoteHostLength  int
	HasDocumentation  bool
	FilterLength      int
	HasCustomAddress  bool
	HasCustomBase     bool
	HasHostname       bool
	RunningContainers int
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

	row := RowType{
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

var channel chan RowType

func main() {
	channel = make(chan RowType)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for {
			file, err := os.Create(fmt.Sprintf("data/data-%d.temp", time.Now().Unix()))
			if err != nil {
				log.Fatal(err)
			}
			writer := parquet.NewGenericWriter[RowType](file, parquet.Compression(&parquet.Zstd))
			i := 0
			closed := false

		loop:
			for {
				context, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
				defer cancel()
				select {
				case <-context.Done():
					if i > 0 {
						break loop
					}
					continue
				case row, ok := <-channel:
					if ok {
						i++
						// log.Printf("Writing row %+v", row)
						writer.Write([]RowType{row})
					} else {
						closed = true
						break loop
					}
				}

				if i > 100000 {
					break
				}
			}

			if i > 0 {
				log.Println("Writing to file")
				writer.Close()
				file.Close()
				os.Rename(file.Name(), fmt.Sprintf("data/data-%s.parquet", time.Now().Format(time.RFC3339)))
			} else {
				log.Println("Removing empty file")
				file.Close()
				os.Remove(file.Name())
			}

			if closed {
				log.Println("Closing")
				break
			}
		}

		wg.Done()
	}()

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
	close(channel)
	log.Println("Waiting")
	wg.Wait()
	log.Println("Done")
}
