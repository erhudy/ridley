package main

import (
	"bytes"
	"io"
	"net/http"
	"time"

	"go.uber.org/zap"
)

type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

type RemoteWriteHandler struct {
	client      HTTPDoer
	connTracker *ConnTracker
	target      string
	addlHeaders map[string]string
	logger      *zap.SugaredLogger
	metrics     *RidleyMetrics
}

func (rwh *RemoteWriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	rwh.logger.Debugw("remote addr", "ip:port", r.RemoteAddr)
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
		rwh.logger.Errorw("error processing request", "error", err.Error())
		rwh.metrics.FailedIncomingRequestsTotal.WithLabelValues().Inc()
		return
	}
	replica := r.Header.Get(HEADER_X_RIDLEY_REPLICA)
	if replica == "" {
		w.WriteHeader(http.StatusBadRequest)
		rwh.logger.Errorw("missing required header", "header", HEADER_X_RIDLEY_REPLICA)
		rwh.metrics.FailedIncomingRequestsTotal.WithLabelValues().Inc()
		return
	}

	rwh.HandleRequest(w, r.Header, body, now, replica)
}

func (rwh *RemoteWriteHandler) HandleRequest(w http.ResponseWriter, headers http.Header, body []byte, recievedTime time.Time, replica string) {
	if rwh.connTracker.TrySetActiveConnection(replica, recievedTime) {
		rwh.metrics.IncomingRequestsTotal.WithLabelValues(replica).Inc()
		rwh.logger.Debugw("handling request", "replica", replica)

		req, err := http.NewRequest(http.MethodPost, rwh.target, bytes.NewReader(body))
		if err != nil {
			rwh.logger.Errorw("failed to create HTTP request", "error", err.Error())
			rwh.metrics.SendErrorsTotal.WithLabelValues("599").Inc()
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		req.Header = headers.Clone()
		for headerKey, headerValue := range rwh.addlHeaders {
			req.Header.Set(headerKey, headerValue)
		}

		resp, err := rwh.client.Do(req)
		if err != nil {
			rwh.logger.Errorw("failed to send HTTP request", "error", err)
			rwh.metrics.SendErrorsTotal.WithLabelValues("599").Inc()
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		defer func() {
			if closeErr := resp.Body.Close(); closeErr != nil {
				rwh.logger.Warnw("failed to close response body", "error", closeErr)
			}
		}()
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			rwh.logger.Errorw("failed to read response body", "error", err)
			w.WriteHeader(resp.StatusCode)
			return
		}
		w.WriteHeader(resp.StatusCode)
		if _, writeErr := w.Write(respBody); writeErr != nil {
			rwh.logger.Warnw("failed to write response body", "error", writeErr)
		}
	} else {
		rwh.logger.Debugw("discarding request", "replica", replica)
		w.WriteHeader(http.StatusAccepted)
	}
}
