package main

import (
	"net/http"
	"os"
	"sync"
	"time"
)

type RequestWithTimestamp struct {
	requestBody    []byte
	requestHeaders http.Header
	timestamp      time.Time
}

type ConnTracker struct {
	activeConnection           string
	activeLastRequestTimestamp *time.Time
	conntrackTable             map[string]chan RequestWithTimestamp
	mutex                      *sync.Mutex
}

type RemoteWriteHandler struct {
	client      *http.Client
	connTracker *ConnTracker
	quitChan    chan os.Signal
	requestChan chan RequestWithTimestamp
	sendChan    chan RequestWithTimestamp
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
