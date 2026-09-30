package writer

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/amir20/drain/internal"
	"github.com/lib/pq"
)

func TestLiveBeaconWhitelistsAndClips(t *testing.T) {
	long := strings.Repeat("é", liveMaxString) // two bytes each, so the cut lands mid-rune
	e := event(t, "usage", "abc", `{"version":"v11.1.3","remoteIP":"secret","activeMinutes":"11-60","browser":"`+long+`"}`)
	e.RemoteIP = "secret"

	b, err := json.Marshal(liveBeacon(e))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "secret") {
		t.Fatalf("payload leaks remoteIP: %s", b)
	}
	var got LiveBeacon
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.ActiveMinutes != "11-60" || got.Version != "v11.1.3" || got.Client != "abc" {
		t.Fatalf("payload = %+v", got)
	}
	if len(got.UserAgent) > liveMaxString || !utf8.ValidString(got.UserAgent) {
		t.Fatalf("ua not clipped cleanly: %d bytes, valid=%v", len(got.UserAgent), utf8.ValidString(got.UserAgent))
	}
}

// A committed batch is announced on LiveChannel, one notification per beacon.
func TestFlushNotifies(t *testing.T) {
	p := testWriter(t)
	l := pq.NewListener(os.Getenv("DRAIN_TEST_DATABASE_URL"), time.Second, time.Second, nil)
	t.Cleanup(func() { l.Close() })
	if err := l.Listen(LiveChannel); err != nil {
		t.Fatal(err)
	}

	p.flush([]internal.Event{
		event(t, "events", "a", `{"browser":"Mozilla/5.0 (Macintosh) Chrome/1"}`),
		event(t, "start", "b", `{"mode":"server"}`),
	})

	seen := map[string]LiveBeacon{}
	for len(seen) < 2 {
		select {
		case n := <-l.Notify:
			var b LiveBeacon
			if err := json.Unmarshal([]byte(n.Extra), &b); err != nil {
				t.Fatal(err)
			}
			seen[b.Client] = b
		case <-time.After(5 * time.Second):
			t.Fatalf("got %d notifications, want 2", len(seen))
		}
	}
	if seen["a"].Name != "events" || seen["b"].Mode != "server" {
		t.Fatalf("notifications = %+v", seen)
	}
}
