package main

import (
	"sync"
	"time"
)

type ConnTracker struct {
	ActiveConnection      *string
	connectionTimeout     time.Duration
	activeLastRequestTime time.Time
	mutex                 *sync.Mutex
}

func NewConnTracker(timeout time.Duration) *ConnTracker {
	return &ConnTracker{
		ActiveConnection:      nil,
		connectionTimeout:     timeout,
		activeLastRequestTime: time.Time{},
		mutex:                 &sync.Mutex{},
	}
}

func (ct *ConnTracker) TrySetActiveConnection(replica string, requestTime time.Time) bool {
	ct.mutex.Lock()
	defer ct.mutex.Unlock()
	if ct.ActiveConnection == nil {
		logger.Infow("setting first active connection", "replica", replica)
		ct.ActiveConnection = &replica
		ct.activeLastRequestTime = requestTime
		return true
	}

	if *ct.ActiveConnection == replica {
		logger.Debugw("replica already active", "replica", replica)
		ct.activeLastRequestTime = requestTime
		return true
	}

	if requestTime.Sub(ct.activeLastRequestTime) > ct.connectionTimeout {
		logger.Infow("timeout exceeded since last replica request, switching active replica", "prevReplica", *ct.ActiveConnection, "newReplica", replica)
		ct.ActiveConnection = &replica
		ct.activeLastRequestTime = requestTime
		return true
	}

	return false
}
