package web

import (
	"encoding/json"
	"net/http"
	"os"
	"time"

	"github.com/amir20/drain/internal"
	"go.uber.org/zap"
)

// maxBeaconBytes is far above any real beacon, which is a few hundred bytes.
const maxBeaconBytes = 64 << 10

func NewHTTPServer(channel chan<- internal.Event, ips *IPHasher, logger *zap.SugaredLogger) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/event", eventHandler(channel, ips, logger))

	addr, exists := os.LookupEnv("DRAIN_ADDR")
	if !exists {
		addr = ":4000"
	}
	// A beacon is a few hundred bytes, so these are generous. Without them a client that
	// trickles its headers or body holds a connection and a goroutine open indefinitely.
	return &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
}

// eventHandler decodes a beacon into the named fields and also keeps the raw body, so
// a field a newer Dozzle sends lands in the metadata JSONB without drain knowing it.
func eventHandler(channel chan<- internal.Event, ips *IPHasher, logger *zap.SugaredLogger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var raw json.RawMessage
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBeaconBytes)).Decode(&raw); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var row internal.Event
		if err := json.Unmarshal(raw, &row); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		// Server-side fields are never taken from the client. The address is hashed
		// before it goes anywhere, so the raw IP never reaches the database.
		//
		// The first X-Forwarded-For entry is only the real client because Traefik, by
		// default, discards the header from untrusted sources and writes its own. Setting
		// forwardedHeaders.insecure or trustedIPs on the entrypoint would let a client
		// choose its own entry here.
		row.CreatedAt = time.Now()
		row.RemoteIP = ips.Hash(r.Header.Get("X-Forwarded-For"))
		row.Raw = raw

		logger.Debugf("Received event: %+v", row)

		channel <- row

		w.WriteHeader(http.StatusCreated)
	}
}
