package main

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func newTestConnTracker(timeout time.Duration) *ConnTracker {
	return NewConnTracker(timeout, zap.NewNop().Sugar())
}

func TestConnTracker_FirstConnection(t *testing.T) {
	ct := newTestConnTracker(30 * time.Second)
	result := ct.TrySetActiveConnection("replica-1", time.Now())
	assert.True(t, result)
	name := ct.ActiveConnectionName()
	assert.NotNil(t, name)
	assert.Equal(t, "replica-1", *name)
}

func TestConnTracker_SameReplicaAlwaysAccepted(t *testing.T) {
	ct := newTestConnTracker(30 * time.Second)
	t1 := time.Now()
	ct.TrySetActiveConnection("replica-1", t1)
	t2 := t1.Add(5 * time.Second)
	result := ct.TrySetActiveConnection("replica-1", t2)
	assert.True(t, result)
	name := ct.ActiveConnectionName()
	assert.Equal(t, "replica-1", *name)
}

func TestConnTracker_DifferentReplicaRejectedWithinTimeout(t *testing.T) {
	ct := newTestConnTracker(30 * time.Second)
	t1 := time.Now()
	ct.TrySetActiveConnection("replica-1", t1)
	t2 := t1.Add(5 * time.Second)
	result := ct.TrySetActiveConnection("replica-2", t2)
	assert.False(t, result)
	name := ct.ActiveConnectionName()
	assert.Equal(t, "replica-1", *name)
}

func TestConnTracker_DifferentReplicaAcceptedAfterTimeout(t *testing.T) {
	ct := newTestConnTracker(30 * time.Second)
	t1 := time.Now()
	ct.TrySetActiveConnection("replica-1", t1)
	t2 := t1.Add(31 * time.Second)
	result := ct.TrySetActiveConnection("replica-2", t2)
	assert.True(t, result)
	name := ct.ActiveConnectionName()
	assert.Equal(t, "replica-2", *name)
}

func TestConnTracker_TimeoutExactBoundary(t *testing.T) {
	timeout := 30 * time.Second
	t1 := time.Now()

	// Exactly at boundary → false (> not >=)
	ct := newTestConnTracker(timeout)
	ct.TrySetActiveConnection("replica-1", t1)
	result := ct.TrySetActiveConnection("replica-2", t1.Add(timeout))
	assert.False(t, result)

	// boundary+1ns → true
	ct2 := newTestConnTracker(timeout)
	ct2.TrySetActiveConnection("replica-1", t1)
	result2 := ct2.TrySetActiveConnection("replica-2", t1.Add(timeout+1))
	assert.True(t, result2)
}

func TestConnTracker_ConcurrentAccess(t *testing.T) {
	ct := newTestConnTracker(30 * time.Second)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			replica := "replica-1"
			if idx%2 != 0 {
				replica = "replica-2"
			}
			ct.TrySetActiveConnection(replica, time.Now())
		}(i)
	}
	wg.Wait()
	// Verify no panic or data race; active connection must be set to one of the two replicas
	name := ct.ActiveConnectionName()
	assert.NotNil(t, name)
	assert.Contains(t, []string{"replica-1", "replica-2"}, *name)
}
