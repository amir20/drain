package internal

import (
	"encoding/json"
	"time"
)

// Event is one beacon from a Dozzle install. The named fields are the ones drain
// itself needs (the row's name, time and client id). Raw keeps the whole payload as
// it arrived, so a field Dozzle adds later still reaches the metadata JSONB without
// a change here.
type Event struct {
	Name              string    `json:"name"`
	CreatedAt         time.Time `json:"createdAt"`
	AuthProvider      string    `json:"authProvider"`
	RemoteIP          string    `json:"remoteIP"`
	Version           string    `json:"version"`
	Clients           int       `json:"clients"`
	Browser           string    `json:"browser"`
	FilterLength      int       `json:"filterLength"`
	HasCustomAddress  bool      `json:"hasCustomAddress"`
	HasCustomBase     bool      `json:"hasCustomBase"`
	HasHostname       bool      `json:"hasHostname"`
	HasActions        bool      `json:"hasActions"`
	HasShell          bool      `json:"hasShell"`
	RunningContainers int       `json:"runningContainers"`
	IsSwarmMode       bool      `json:"isSwarmMode"`
	ServerVersion     string    `json:"serverVersion"`
	ServerID          string    `json:"serverID"`
	Mode              string    `json:"mode"`
	RemoteAgents      int       `json:"remoteAgents"`
	RemoteClients     int       `json:"remoteClients"`
	SubCommand        string    `json:"subCommand"`
	// FileAgents counts agents added from the Dozzle UI (saved in /data/dozzle.yml),
	// as opposed to DOZZLE_REMOTE_AGENT.
	FileAgents int `json:"fileAgents"`

	Raw json.RawMessage `json:"-"`
}

// Metadata is the JSONB stored with the beacon: every field of the payload as sent,
// with the named fields laid over it so the server-side ones (createdAt, remoteIP)
// always come from drain and never from the client.
func (e Event) Metadata() ([]byte, error) {
	known, err := json.Marshal(e)
	if err != nil {
		return nil, err
	}
	if len(e.Raw) == 0 {
		return known, nil
	}

	merged := map[string]json.RawMessage{}
	if err := json.Unmarshal(e.Raw, &merged); err != nil {
		// Raw is only ever set from a body that already decoded as an object, so this
		// is not expected. The named fields are still worth keeping.
		return known, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(known, &fields); err != nil {
		return nil, err
	}
	for k, v := range fields {
		merged[k] = v
	}
	return json.Marshal(merged)
}
