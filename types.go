package main

import (
	"net/http"
	"sync"
	"time"
)

type RequestWthTimestamp struct {
	requestBody    []byte
	requestHeaders http.Header
	timestamp      time.Time
}

type ConnTracker struct {
	activeConnection           string
	activeLastRequestTimestamp *time.Time
	conntrackTable             map[string]chan RequestWthTimestamp
	mutex                      *sync.Mutex
}

type RemoteWriteHandler struct {
	client      *http.Client
	connTracker *ConnTracker
	quitChan    chan struct{}
	requestChan chan RequestWthTimestamp
	sendChan    chan RequestWthTimestamp
}

type RemoteAddr string
type Status int

const (
	STATUS_UNKNOWN Status = iota // put UNKNOWN as 0 so an empty int won't evaluate to active
	STATUS_ACTIVE
	STATUS_BACKUP
	STATUS_STALE
)

const HEADER_X_RIDLEY_REPLICA string = "X-Ridley-Replica"
