package writer

import (
	"slices"
	"testing"
	"time"

	"github.com/amir20/drain/internal"
)

// run feeds events through batch and returns the batch sizes flush saw, in order.
func run(t *testing.T, size int, interval time.Duration, feed func(chan<- internal.Event)) []int {
	t.Helper()
	in := make(chan internal.Event)
	var sizes []int
	done := make(chan struct{})
	go func() {
		batch(in, size, interval, func(b []internal.Event) { sizes = append(sizes, len(b)) })
		close(done)
	}()
	feed(in)
	close(in)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("batch did not return after its input closed")
	}
	return sizes
}

func TestBatchFlushesWhenFull(t *testing.T) {
	// An hour-long interval, so only size can trigger a flush before close.
	sizes := run(t, 3, time.Hour, func(in chan<- internal.Event) {
		for range 7 {
			in <- internal.Event{}
		}
	})
	if want := []int{3, 3, 1}; !slices.Equal(sizes, want) {
		t.Fatalf("batches = %v, want %v", sizes, want)
	}
}

func TestBatchFlushesOnInterval(t *testing.T) {
	sizes := run(t, 100, 10*time.Millisecond, func(in chan<- internal.Event) {
		in <- internal.Event{}
		in <- internal.Event{}
		// Long enough for several ticks: the pair must go out without the batch filling.
		time.Sleep(100 * time.Millisecond)
	})
	if len(sizes) != 1 || sizes[0] != 2 {
		t.Fatalf("batches = %v, want [2] from the ticker", sizes)
	}
}

func TestBatchFlushesRemainderOnClose(t *testing.T) {
	sizes := run(t, 100, time.Hour, func(in chan<- internal.Event) {
		in <- internal.Event{}
	})
	if len(sizes) != 1 || sizes[0] != 1 {
		t.Fatalf("batches = %v, want the last partial batch flushed on close", sizes)
	}
}
