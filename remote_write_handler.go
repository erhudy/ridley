package main

import (
	"bytes"
	"io"
	"net/http"
	"time"
)

type RemoteWriteHandler struct {
	client      *http.Client
	connTracker *ConnTracker
}

func (rwh *RemoteWriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	logger.Debugw("remote addr", "ip:port", r.RemoteAddr)
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
		logger.Errorw("error processing request", "error", err.Error())
		metric_FailedIncomingRequestsTotal.WithLabelValues().Inc()
		return
	}
	replica := r.Header.Get(HEADER_X_RIDLEY_REPLICA)
	if replica == "" {
		w.WriteHeader(http.StatusBadRequest)
		logger.Errorw("missing required header", "header", HEADER_X_RIDLEY_REPLICA)
		metric_FailedIncomingRequestsTotal.WithLabelValues().Inc()
		return
	}

	rwh.HandleRequest(w, r.Header, body, now, replica)
}

func (rwh *RemoteWriteHandler) HandleRequest(w http.ResponseWriter, headers http.Header, body []byte, recievedTime time.Time, replica string) {
	if rwh.connTracker.TrySetActiveConnection(replica, recievedTime) {
		metric_IncomingRequestsTotal.WithLabelValues(replica).Inc()
		logger.Debugw("handling request", "replica", replica)

		target := v.GetString(FLAG_NAME_TARGET)
		addlHeaders := v.GetStringMapString(FLAG_NAME_TARGET_HEADERS)
		req, err := http.NewRequest(http.MethodPost, target, bytes.NewReader(body))
		if err != nil {
			logger.Errorw("failed to create HTTP request", "error", err.Error())
			metric_SendErrorsTotal.WithLabelValues("599").Inc()
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		req.Header = headers.Clone()
		for headerKey, headerValue := range addlHeaders {
			req.Header.Set(headerKey, headerValue)
		}

		resp, err := rwh.client.Do(req)
		if err != nil {
			logger.Errorw("failed to send HTTP request", "error", err)
			metric_SendErrorsTotal.WithLabelValues("599").Inc()
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		defer resp.Body.Close()
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			logger.Errorw("failed to read response body", "error", err)
			w.WriteHeader(resp.StatusCode)
			return
		}
		w.WriteHeader(resp.StatusCode)
		w.Write(respBody)
	} else {
		logger.Debugw("discarding request", "replica", replica)
		w.WriteHeader(http.StatusAccepted)
	}
}
