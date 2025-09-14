package main

import (
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
)

var logger *zap.Logger

var target string = "http://localhost:9090/api/v1/write"
var timeoutDuration time.Duration = time.Second * 30

func init() {
	var err error
	logger, err = zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
}

func main() {
	client := http.Client{}
	requestChan := make(chan RequestWthTimestamp)
	quitChan := make(chan struct{})
	sendChan := make(chan RequestWthTimestamp)

	rwh := RemoteWriteHandler{
		client: &client,
		connTracker: &ConnTracker{
			activeLastRequestTimestamp: nil,
			conntrackTable:             make(map[string]chan RequestWthTimestamp),
			mutex:                      &sync.Mutex{},
		},
		quitChan:    quitChan,
		requestChan: requestChan,
		sendChan:    sendChan,
	}

	// make sure to start dispatcher before starting the HTTP server so that channel does not deadlock
	go rwh.Dispatch()

	mux := http.NewServeMux()
	mux.Handle("/write", &rwh)

	server := &http.Server{
		Addr:    "0.0.0.0:8080",
		Handler: mux,
	}

	logger.Info("started on 0.0.0.0:8080")

	err := server.ListenAndServe()
	quitChan <- struct{}{}
	logger.Fatal(err.Error())
}
