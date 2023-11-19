package web

import (
	"encoding/json"
	"net/http"

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

func NewHTTPServer(channel chan<- writer.WriterRow) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/event", func(w http.ResponseWriter, r *http.Request) {
		var beaconEvent BeaconEvent
		err := json.NewDecoder(r.Body).Decode(&beaconEvent)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		row := writer.WriterRow{
			Version:           beaconEvent.Version,
			Browser:           beaconEvent.Browser,
			AuthProvider:      beaconEvent.AuthProvider,
			HasDocumentation:  beaconEvent.HasDocumentation,
			FilterLength:      beaconEvent.FilterLength,
			RemoteHostLength:  beaconEvent.RemoteHostLength,
			HasCustomAddress:  beaconEvent.HasCustomAddress,
			HasCustomBase:     beaconEvent.HasCustomBase,
			HasHostname:       beaconEvent.HasHostname,
			RunningContainers: beaconEvent.RunningContainers,
		}
		row.RemoteIP = r.Header.Get("X-Forwarded-For")
		channel <- row

		w.WriteHeader(http.StatusCreated)
	})

	return &http.Server{
		Addr:    ":4000",
		Handler: mux,
	}
}
