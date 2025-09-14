package main

import (
	"net/http"
	"time"
)

type RequestWthTimestamp struct {
	requestBody    []byte
	requestHeaders http.Header
	timestamp      time.Time
}

type RemoteWriteHandler struct {
	client         *http.Client
	conntrackTable map[string]chan RequestWthTimestamp
	quitChan       chan struct{}
	requestChan    chan RequestWthTimestamp
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
