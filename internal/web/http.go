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
	Name              string `json:"name"`
	Version           string `json:"version"`
	Browser           string `json:"browser"`
	AuthProvider      string `json:"authProvider"`
	FilterLength      int    `json:"filterLength"`
	Clients           int    `json:"clients"`
	HasCustomAddress  bool   `json:"hasCustomAddress"`
	HasCustomBase     bool   `json:"hasCustomBase"`
	HasHostname       bool   `json:"hasHostname"`
	RunningContainers int    `json:"runningContainers"`
	HasActions        bool   `json:"hasActions"`
	IsSwarmMode       bool   `json:"isSwarmMode"`
	ServerVersion     string `json:"serverVersion"`
	ServerID          string `json:"serverID"`
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
			Name:              beaconEvent.Name,
			CreatedAt:         time.Now(),
			Version:           beaconEvent.Version,
			Browser:           beaconEvent.Browser,
			AuthProvider:      beaconEvent.AuthProvider,
			FilterLength:      beaconEvent.FilterLength,
			Clients:           beaconEvent.Clients,
			HasCustomAddress:  beaconEvent.HasCustomAddress,
			HasCustomBase:     beaconEvent.HasCustomBase,
			HasHostname:       beaconEvent.HasHostname,
			RunningContainers: beaconEvent.RunningContainers,
			HasActions:        beaconEvent.HasActions,
			IsSwarmMode:       beaconEvent.IsSwarmMode,
			ServerVersion:     beaconEvent.ServerVersion,
			ServerID:          beaconEvent.ServerID,
		}
		row.RemoteIP = r.Header.Get("X-Forwarded-For")

		logger.Debugf("Received event: %+v", row)

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
