package main

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

type ConnTracker struct {
	activeConnection      *string
	connectionTimeout     time.Duration
	activeLastRequestTime time.Time
	mutex                 *sync.Mutex
	logger                *zap.SugaredLogger
}

func NewConnTracker(timeout time.Duration, log *zap.SugaredLogger) *ConnTracker {
	return &ConnTracker{
		activeConnection:      nil,
		connectionTimeout:     timeout,
		activeLastRequestTime: time.Time{},
		mutex:                 &sync.Mutex{},
		logger:                log,
	}
}

func (ct *ConnTracker) ActiveConnectionName() *string {
	ct.mutex.Lock()
	defer ct.mutex.Unlock()
	return ct.activeConnection
}

func (ct *ConnTracker) TrySetActiveConnection(replica string, requestTime time.Time) bool {
	ct.mutex.Lock()
	defer ct.mutex.Unlock()
	if ct.activeConnection == nil {
		ct.logger.Infow("setting first active connection", "replica", replica)
		ct.activeConnection = &replica
		ct.activeLastRequestTime = requestTime
		return true
	}

	if *ct.activeConnection == replica {
		ct.logger.Debugw("replica already active", "replica", replica)
		ct.activeLastRequestTime = requestTime
		return true
	}

	if requestTime.Sub(ct.activeLastRequestTime) > ct.connectionTimeout {
		ct.logger.Infow("timeout exceeded since last replica request, switching active replica", "prevReplica", *ct.activeConnection, "newReplica", replica)
		ct.activeConnection = &replica
		ct.activeLastRequestTime = requestTime
		return true
	}

	return false
}
