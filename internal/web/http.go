package web

import (
	"encoding/json"
	"net/http"
	"os"
	"time"

	"github.com/amir20/drain/internal"
	"go.uber.org/zap"
)

type BeaconEvent struct {
	Version           string `json:"version"`
	Browser           string `json:"browser"`
	AuthProvider      string `json:"authProvider"`
	HasDocumentation  bool   `json:"hasDocumentation"`
	FilterLength      int    `json:"filterLength"`
	Clients           int    `json:"clients"`
	HasCustomAddress  bool   `json:"hasCustomAddress"`
	HasCustomBase     bool   `json:"hasCustomBase"`
	HasHostname       bool   `json:"hasHostname"`
	RunningContainers int    `json:"runningContainers"`
}

func NewHTTPServer(channel chan<- internal.Event, logger *zap.SugaredLogger) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/event", func(w http.ResponseWriter, r *http.Request) {
		var beaconEvent BeaconEvent
		err := json.NewDecoder(r.Body).Decode(&beaconEvent)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		row := internal.Event{
			CreatedAt:         time.Now(),
			Version:           beaconEvent.Version,
			Browser:           beaconEvent.Browser,
			AuthProvider:      beaconEvent.AuthProvider,
			HasDocumentation:  beaconEvent.HasDocumentation,
			FilterLength:      beaconEvent.FilterLength,
			Clients:           beaconEvent.Clients,
			HasCustomAddress:  beaconEvent.HasCustomAddress,
			HasCustomBase:     beaconEvent.HasCustomBase,
			HasHostname:       beaconEvent.HasHostname,
			RunningContainers: beaconEvent.RunningContainers,
		}
		row.RemoteIP = r.Header.Get("X-Forwarded-For")
		channel <- row

		w.WriteHeader(http.StatusCreated)
	})

	addr, exists := os.LookupEnv("DRAIN_ADDR")
	if !exists {
		addr = ":4000"
	}
	return &http.Server{
		Addr:    addr,
		Handler: mux,
	}
}