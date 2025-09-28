package main

import (
	"time"

	"go.uber.org/zap"
)

func (ct *ConnTracker) GetOrCreate(replica string) (chan RequestWithTimestamp, bool) {
	ct.mutex.Lock()
	defer ct.mutex.Unlock()
	if requestQueue, exists := ct.conntrackTable[replica]; exists {
		return requestQueue, true
	}
	logger.Info("creating new replica tracker", zap.String("replica", replica))
	ct.conntrackTable[replica] = make(chan RequestWithTimestamp, 100)
	if ct.activeConnection == "" {
		logger.Info("setting first observed replica to active", zap.String("replica", replica))
		ct.activeConnection = replica
	}
	return ct.conntrackTable[replica], false
}

func (ct *ConnTracker) SetActiveConnection(replica string) {
	ct.mutex.Lock()
	defer ct.mutex.Unlock()
	ct.activeConnection = replica
}

func (ct *ConnTracker) IsReplicaActive(replica string) bool {
	ct.mutex.Lock()
	defer ct.mutex.Unlock()
	return ct.activeConnection == replica
}

func (ct *ConnTracker) GetActiveLastRequestTimestamp() *time.Time {
	ct.mutex.Lock()
	defer ct.mutex.Unlock()
	return ct.activeLastRequestTimestamp
}

func (ct *ConnTracker) SetActiveLastRequestTimestamp(t time.Time) {
	ct.mutex.Lock()
	defer ct.mutex.Unlock()
	ct.activeLastRequestTimestamp = &t
}
