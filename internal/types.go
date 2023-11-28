package internal

import "time"

type Event struct {
	CreatedAt         time.Time `json:"createdAt"`
	AuthProvider      string    `json:"authProvider"`
	RemoteIP          string    `json:"remoteIp"`
	Version           string    `json:"version"`
	Clients           int       `json:"clients"`
	HasDocumentation  bool      `json:"hasDocumentation"`
	FilterLength      int       `json:"filterLength"`
	HasCustomAddress  bool      `json:"hasCustomAddress"`
	HasCustomBase     bool      `json:"hasCustomBase"`
	HasHostname       bool      `json:"hasHostname"`
	RunningContainers int       `json:"runningContainers"`
	Browser           string    `json:"browser"`
}
