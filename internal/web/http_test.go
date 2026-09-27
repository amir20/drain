package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/amir20/drain/internal"
	"go.uber.org/zap"
)

func post(t *testing.T, body string) (*httptest.ResponseRecorder, chan internal.Event) {
	t.Helper()
	channel := make(chan internal.Event, 1)
	req := httptest.NewRequest(http.MethodPost, "/event", strings.NewReader(body))
	req.Header.Set("X-Forwarded-For", "203.0.113.7")
	rec := httptest.NewRecorder()
	eventHandler(channel, zap.NewNop().Sugar())(rec, req)
	return rec, channel
}

func TestEventHandlerKeepsUnknownFields(t *testing.T) {
	rec, channel := post(t, `{"name":"start","serverID":"abc","remoteAgents":2,"fileAgents":3,"someNewField":3,"remoteIP":"1.2.3.4"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, body %q", rec.Code, rec.Body.String())
	}
	row := <-channel

	if row.Name != "start" || row.ServerID != "abc" || row.RemoteAgents != 2 || row.FileAgents != 3 {
		t.Fatalf("named fields not decoded: %+v", row)
	}

	data, err := row.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	var meta map[string]any
	if err := json.Unmarshal(data, &meta); err != nil {
		t.Fatal(err)
	}

	if meta["someNewField"] != float64(3) {
		t.Errorf("someNewField = %v, want 3", meta["someNewField"])
	}
	if meta["fileAgents"] != float64(3) {
		t.Errorf("fileAgents = %v, want 3", meta["fileAgents"])
	}
	// The keys the drain_* SQL helpers read must not change.
	if meta["remoteAgents"] != float64(2) || meta["serverID"] != "abc" || meta["name"] != "start" {
		t.Errorf("known keys changed: %v", meta)
	}
	if _, ok := meta["createdAt"]; !ok {
		t.Error("createdAt missing")
	}
	// Server-side fields come from drain, never the client.
	if meta["remoteIP"] != "203.0.113.7" {
		t.Errorf("remoteIP = %v, want the forwarded address", meta["remoteIP"])
	}
	if _, ok := meta["Raw"]; ok {
		t.Error("raw payload leaked into metadata as its own key")
	}
}

func TestEventHandlerRejectsBadBodies(t *testing.T) {
	for name, body := range map[string]string{
		"not json":   `nope`,
		"not object": `[1,2]`,
		"too large":  `{"name":"` + strings.Repeat("a", maxBeaconBytes) + `"}`,
	} {
		t.Run(name, func(t *testing.T) {
			rec, _ := post(t, body)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", rec.Code)
			}
		})
	}
}
