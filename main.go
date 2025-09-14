package main

import (
	"net/http"

	"go.uber.org/zap"
)

var logger *zap.Logger

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

	rwh := RemoteWriteHandler{
		client:         &client,
		conntrackTable: make(map[string]chan RequestWthTimestamp),
		quitChan:       quitChan,
		requestChan:    requestChan,
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
