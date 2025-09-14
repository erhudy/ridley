package main

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
)

func (rwh *RemoteWriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	logger.Debug("remote addr", zap.String("ip:port", r.RemoteAddr))
	// logger.Info("headers", zap.Any("headers", r.Header))
	/* example headers:
	{
		"Content-Encoding":["snappy"],
		"Content-Length":["13317"],
		"Content-Type":["application/x-protobuf"],
		"User-Agent":["Prometheus/2.54.1"],
		"X-Prometheus-Remote-Write-Version":["0.1.0"]
	}
	*/
	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "error processing request: %s", err.Error())
		return
	}
	if r.Header.Get(HEADER_X_RIDLEY_REPLICA) == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "%s header must be set on every request", HEADER_X_RIDLEY_REPLICA)
		return
	}
	req := RequestWthTimestamp{
		requestBody:    body,
		requestHeaders: r.Header.Clone(),
		timestamp:      now,
	}

	rwh.requestChan <- req
	w.WriteHeader(http.StatusAccepted)
}

func processReplica(replicaChan chan RequestWthTimestamp, conntrackMutex *sync.Mutex) {
	for x := range replicaChan {
		logger.Debug("processReplica handling request", zap.String("replica", x.requestHeaders.Get(HEADER_X_RIDLEY_REPLICA)))
	}
}

func (rwh *RemoteWriteHandler) Dispatch() {
	for {
		select {
		case x := <-rwh.requestChan:
			replicaHeader := x.requestHeaders.Get(HEADER_X_RIDLEY_REPLICA)
			logger.Debug("dispatch handling request", zap.String("replica", replicaHeader))

			conntrackMutex := sync.Mutex{}
			var conn chan RequestWthTimestamp
			var ok bool
			conn, ok = rwh.conntrackTable[replicaHeader]
			if !ok {
				logger.Info("creating new replica tracker", zap.String("replica", replicaHeader))
				conn = make(chan RequestWthTimestamp)
				conntrackMutex.Lock()
				rwh.conntrackTable[replicaHeader] = conn
				conntrackMutex.Unlock()
				go processReplica(conn, &conntrackMutex)
			}
			conn <- x
		case <-rwh.quitChan:
			logger.Info("received shutdown signal")
		}
	}

}
