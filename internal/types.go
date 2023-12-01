package internal

import "time"

type Event struct {
	Name              string
	CreatedAt         time.Time
	AuthProvider      string
	RemoteIP          string
	Version           string
	Clients           int
	HasDocumentation  bool
	FilterLength      int
	HasCustomAddress  bool
	HasCustomBase     bool
	HasHostname       bool
	RunningContainers int
	Browser           string
	HasActions        bool
}
