package internal

import "time"

type Event struct {
	Name              string    `json:"name"`
	CreatedAt         time.Time `json:"createdAt"`
	AuthProvider      string    `json:"authProvider"`
	RemoteIP          string    `json:"remoteIP"`
	Version           string    `json:"version"`
	Clients           int       `json:"clients"`
	FilterLength      int       `json:"filterLength"`
	HasCustomAddress  bool      `json:"hasCustomAddress"`
	HasCustomBase     bool      `json:"hasCustomBase"`
	HasHostname       bool      `json:"hasHostname"`
	RunningContainers int       `json:"runningContainers"`
	Browser           string    `json:"browser"`
	HasActions        bool      `json:"hasActions"`
	IsSwarmMode       bool      `json:"isSwarmMode"`
	ServerVersion     string    `json:"serverVersion"`
	ServerID          string    `json:"serverID"`
	Mode              string    `json:"mode"`
	RemoteAgents      int       `json:"remoteAgents"`
	RemoteClients     int       `json:"remoteClients"`
	SubCommand        string    `json:"subCommand"`
}
