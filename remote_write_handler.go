package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/xorcare/pointer"
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

func (rwh *RemoteWriteHandler) processReplica(replicaChan chan RequestWthTimestamp) {
	for request := range replicaChan {
		replica := request.requestHeaders.Get(HEADER_X_RIDLEY_REPLICA)
		logger.Debug("processReplica handling request", zap.String("replica", replica))

		if rwh.connTracker.activeLastRequestTimestamp == nil {
			logger.Info("last request timestamp is nil, still starting up")
		} else {
			logger.Debug("time since last request forwarded", zap.Duration("duration", time.Since(*rwh.connTracker.activeLastRequestTimestamp)))
			if time.Since(*rwh.connTracker.activeLastRequestTimestamp) > timeoutDuration {
				logger.Warn("switching active replica due to timeout of previous active", zap.Duration("timeoutDuration", timeoutDuration))
				if rwh.connTracker.activeConnection == replica {
					continue
				} else {
					rwh.connTracker.activeConnection = replica
				}
			}
		}

		if rwh.connTracker.activeConnection == replica {
			logger.Debug("forwarding request for active replica", zap.String("replica", replica))
			rwh.connTracker.activeLastRequestTimestamp = pointer.Time(time.Now())
			rwh.sendChan <- request
		} else {
			logger.Debug("discarding request", zap.String("replica", replica))
		}
	}
}

func (rwh *RemoteWriteHandler) sendToTarget() {
	for request := range rwh.sendChan {
		req, err := http.NewRequest(http.MethodPost, target, bytes.NewReader(request.requestBody))
		if err != nil {
			panic("kaboom")
		}
		req.Header = request.requestHeaders
		resp, err := rwh.client.Do(req)
		if err != nil {
			panic("more kaboom")
		}
		if resp.StatusCode > 299 {
			logger.Error("got unexpected error code", zap.Int("status", resp.StatusCode))
		}
	}
}

func (rwh *RemoteWriteHandler) Dispatch() {
	go rwh.sendToTarget()
	for {
		select {
		case request := <-rwh.requestChan:
			replicaHeader := request.requestHeaders.Get(HEADER_X_RIDLEY_REPLICA)
			logger.Debug("dispatch handling request", zap.String("replica", replicaHeader))

			var requestQueue chan RequestWthTimestamp
			var ok bool
			requestQueue, ok = rwh.connTracker.Get(replicaHeader)
			if !ok {
				logger.Info("creating new replica tracker", zap.String("replica", replicaHeader))
				var err error
				requestQueue, err = rwh.connTracker.Add(replicaHeader)
				if err != nil {
					panic("tried to add an already existing replica")
				}
				go rwh.processReplica(requestQueue)
			}
			requestQueue <- request
		case <-rwh.quitChan:
			logger.Info("received shutdown signal")
		}
	}

}

func (ct *ConnTracker) Get(replica string) (requestQueue chan RequestWthTimestamp, ok bool) {
	ct.mutex.Lock()
	defer ct.mutex.Unlock()
	requestQueue, ok = ct.conntrackTable[replica]
	return requestQueue, ok
}

func (ct *ConnTracker) Add(replica string) (requestQueue chan RequestWthTimestamp, err error) {
	ct.mutex.Lock()
	defer ct.mutex.Unlock()
	requestQueue = make(chan RequestWthTimestamp)
	ct.conntrackTable[replica] = requestQueue
	if ct.activeConnection == "" {
		logger.Info("setting first observed replica to active", zap.String("replica", replica))
		ct.activeConnection = replica
	}
	return requestQueue, nil
}
