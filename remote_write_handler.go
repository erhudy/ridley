package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.uber.org/zap"
)

func (rwh *RemoteWriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	logger.Debug("remote addr", zap.String("ip:port", r.RemoteAddr))
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
	req := RequestWithTimestamp{
		requestBody:    body,
		requestHeaders: r.Header.Clone(),
		timestamp:      now,
	}

	logger.Debug("writing to requestChan")
	rwh.requestChan <- req
	logger.Debug("wrote to requestChan")
	w.WriteHeader(http.StatusAccepted)
}

func (rwh *RemoteWriteHandler) Dispatch() {
	logger.Debug("starting dispatch")
	go rwh.sendToTarget()
	logger.Debug("chilling before for loop")
	for {
		logger.Debug("tick")
		select {
		case request := <-rwh.requestChan:
			logger.Debug("dispatch got request")
			replicaHeader := request.requestHeaders.Get(HEADER_X_RIDLEY_REPLICA)
			logger.Debug("dispatch handling request", zap.String("replica", replicaHeader))

			requestQueue, exists := rwh.connTracker.GetOrCreate(replicaHeader)
			if !exists {
				logger.Info("spawning new process replica", zap.String("replica", replicaHeader))
				go rwh.processReplica(requestQueue)
			}
			requestQueue <- request
		}
	}
}

func (rwh *RemoteWriteHandler) processReplica(replicaChan chan RequestWithTimestamp) {
	for request := range replicaChan {
		replica := request.requestHeaders.Get(HEADER_X_RIDLEY_REPLICA)
		logger.Debug("processReplica handling request", zap.String("replica", replica))

		lrqTs := rwh.connTracker.GetActiveLastRequestTimestamp()
		if lrqTs == nil {
			logger.Info("last request timestamp is nil, still starting up")
		} else {
			logger.Debug("time since last request forwarded", zap.Duration("duration", time.Since(*lrqTs)))
			if time.Since(*lrqTs) > timeoutDuration {
				logger.Warn("switching active replica due to timeout of previous active", zap.Duration("timeoutDuration", timeoutDuration))
				if rwh.connTracker.IsReplicaActive(replica) {
					continue
				} else {
					rwh.connTracker.SetActiveConnection(replica)
				}
			}
		}

		if rwh.connTracker.IsReplicaActive(replica) {
			logger.Debug("forwarding request for active replica", zap.String("replica", replica))
			rwh.connTracker.SetActiveLastRequestTimestamp(time.Now())
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
			logger.Error("failed to create HTTP request", zap.Error(err))
			continue
		}
		req.Header = request.requestHeaders
		resp, err := rwh.client.Do(req)
		if err != nil {
			logger.Error("failed to send HTTP request", zap.Error(err))
			continue
		}
		if resp.StatusCode > 299 {
			logger.Error("got unexpected error code", zap.Int("status", resp.StatusCode))
			continue
		}
		defer resp.Body.Close()
	}
}
