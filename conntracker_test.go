package main

import (
	"sync"
	"testing"
	"time"
)

func TestGetOrCreateAndActive(t *testing.T) {
	ct := &ConnTracker{
		activeLastRequestTimestamp: time.Time{},
		conntrackTable:             make(map[string]chan RequestWithTimestamp),
		mutex:                      sync.Mutex{},
	}

	q1, exists := ct.GetOrCreate("replica1", 10)
	if exists {
		t.Fatalf("expected new replica to not exist")
	}
	if q1 == nil {
		t.Fatalf("expected non-nil channel")
	}

	// second call should return exists == true
	_, exists = ct.GetOrCreate("replica1", 10)
	if !exists {
		t.Fatalf("expected replica to exist on second call")
	}

	if ct.activeConnection != "replica1" {
		t.Fatalf("expected first observed replica to be active")
	}

	ct.SetActiveConnection("replica2")
	if ct.activeConnection != "replica2" {
		t.Fatalf("SetActiveConnection did not update activeConnection")
	}
}

func TestSetAndGetActiveLastRequestTimestamp(t *testing.T) {
	ct := &ConnTracker{
		activeLastRequestTimestamp: time.Time{},
		conntrackTable:             make(map[string]chan RequestWithTimestamp),
		mutex:                      sync.Mutex{},
	}

	now := time.Now()
	ct.SetActiveLastRequestTimestamp(now)
	got := ct.GetActiveLastRequestTimestamp()
	if got.IsZero() {
		t.Fatalf("expected non-zero timestamp")
	}
	if !got.Equal(now) {
		t.Fatalf("timestamps do not match: got %v want %v", got, now)
	}
}

func TestShutdownClosesChannels(t *testing.T) {
	ct := &ConnTracker{
		activeLastRequestTimestamp: time.Time{},
		conntrackTable:             make(map[string]chan RequestWithTimestamp),
		mutex:                      sync.Mutex{},
	}
	q, _ := ct.GetOrCreate("replicaX", 2)
	select {
	case q <- RequestWithTimestamp{}:
	default:
		t.Fatalf("failed to write to channel")
	}

	ct.Shutdown()

	// after Shutdown, channel should eventually be closed. Drain any buffered values
	timeout := time.After(200 * time.Millisecond)
	for {
		select {
		case _, ok := <-q:
			if !ok {
				// channel closed as expected
				return
			}
			// if we got a value, continue to drain until closed
		case <-timeout:
			t.Fatalf("timed out waiting for closed channel")
		}
	}
}
