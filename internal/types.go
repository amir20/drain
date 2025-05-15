package internal

import "time"

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
}
