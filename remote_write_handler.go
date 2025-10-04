package main

import (
	"bytes"
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
		metric_FailedIncomingRequestsTotal.WithLabelValues().Inc()
		return
	}
	if r.Header.Get(HEADER_X_RIDLEY_REPLICA) == "" {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "%s header must be set on every request", HEADER_X_RIDLEY_REPLICA)
		metric_FailedIncomingRequestsTotal.WithLabelValues().Inc()
		return
	}
	req := RequestWithTimestamp{
		requestBody:    body,
		requestHeaders: r.Header.Clone(),
		timestamp:      now,
	}

	rwh.requestChan <- req
	w.WriteHeader(http.StatusAccepted)
}

func (rwh *RemoteWriteHandler) Dispatch() {
	logger.Debug("starting dispatch")
	wg := sync.WaitGroup{}
	go rwh.sendToTarget()
	stopping := false
	for {
		select {
		case request := <-rwh.requestChan:
			replica := request.requestHeaders.Get(HEADER_X_RIDLEY_REPLICA)
			// for some reason during shutdown we get a request with no replica header set so if replica is "" just skip
			if replica != "" {
				requestQueue, exists := rwh.connTracker.GetOrCreate(replica)
				if !exists {
					logger.Info("spawning new process replica", zap.String("replica", replica))
					wg.Go(func() { rwh.processReplica(requestQueue, replica) })
				}
				requestQueue <- request
			}
			if stopping {
				rwh.connTracker.Shutdown()
				wg.Wait()
				close(rwh.sendChan)
				return
			}
		case <-rwh.quitChan:
			stopping = true
		}
	}
}

func (rwh *RemoteWriteHandler) processReplica(requestQueue chan RequestWithTimestamp, replica string) {
	switchTimeout := v.GetDuration(FLAG_NAME_SWITCH_TIMEOUT)
	for {
		select {
		case request := <-requestQueue:
			logger.Debug("processReplica handling request", zap.String("replica", replica))

			lrqTs := rwh.connTracker.GetActiveLastRequestTimestamp()
			if lrqTs == nil {
				logger.Info("last request timestamp is nil, still starting up")
			} else {
				logger.Debug("time since last request forwarded", zap.Duration("duration", time.Since(*lrqTs)))
				if time.Since(*lrqTs) > switchTimeout {
					logger.Warn("switching active replica due to timeout of previous active", zap.Duration("timeoutDuration", switchTimeout))
					if rwh.connTracker.IsReplicaActive(replica) {
						continue
					} else {
						rwh.connTracker.SetActiveConnection(replica)
					}
				}
			}

			metric_IncomingRequestsTotal.WithLabelValues(replica).Inc()
			if rwh.connTracker.IsReplicaActive(replica) {
				logger.Debug("forwarding request for active replica", zap.String("replica", replica))
				rwh.connTracker.SetActiveLastRequestTimestamp(time.Now())
				rwh.sendChan <- request
			} else {
				logger.Debug("discarding request", zap.String("replica", replica))
			}
		case <-rwh.quitChan:
			logger.Info("stopping process replica", zap.String("replica", replica))
			return
		}
	}

}

func (rwh *RemoteWriteHandler) sendToTarget() {
	target := v.GetString(FLAG_NAME_TARGET)
	addlHeaders := v.GetStringMapString(FLAG_NAME_TARGET_HEADERS)
	for request := range rwh.sendChan {
		req, err := http.NewRequest(http.MethodPost, target, bytes.NewReader(request.requestBody))
		if err != nil {
			logger.Error("failed to create HTTP request", zap.Error(err))
			metric_SendErrorsTotal.WithLabelValues("599").Inc()
			continue
		}
		req.Header = request.requestHeaders
		for headerKey, headerValue := range addlHeaders {
			req.Header.Set(headerKey, headerValue)
		}
		resp, err := rwh.client.Do(req)
		if err != nil {
			logger.Error("failed to send HTTP request", zap.Error(err))
			metric_SendErrorsTotal.WithLabelValues("599").Inc()
			continue
		}
		if resp.StatusCode > 299 {
			logger.Error("got unexpected error code", zap.Int("status", resp.StatusCode))
			metric_SendErrorsTotal.WithLabelValues(fmt.Sprintf("%d", resp.StatusCode)).Inc()
			continue
		}
		resp.Body.Close()
	}
}
