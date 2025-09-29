package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

var logger *zap.Logger

var target string = "http://localhost:9090/api/v1/write"
var timeoutDuration time.Duration = time.Second * 30

// metrics variables
var (
	metric_FailedIncomingRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ridley_failed_incoming_requests_total",
		},
		[]string{},
	)
	metric_IncomingRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ridley_incoming_requests_total",
		},
		[]string{"replica"},
	)
	metric_SendErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "ridley_send_errors_total",
		},
		[]string{"code"},
	)
)

func init() {
	var err error
	logger, err = zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	metric_FailedIncomingRequestsTotal.WithLabelValues().Add(0)
}

func main() {
	client := http.Client{
		Timeout: 10 * time.Second,
	}
	requestChan := make(chan RequestWithTimestamp, 100)
	sendChan := make(chan RequestWithTimestamp, 100)
	quitChan := make(chan struct{}, 100)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	rwh := RemoteWriteHandler{
		client: &client,
		connTracker: &ConnTracker{
			activeLastRequestTimestamp: nil,
			conntrackTable:             make(map[string]chan RequestWithTimestamp),
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
	mux.Handle("/metrics", promhttp.Handler())
	server := &http.Server{
		Addr:    "0.0.0.0:8080",
		Handler: mux,
	}

	go func() {
		logger.Info("starting HTTP server")
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("HTTP server error", zap.Error(err))
		}
		logger.Info("shutting down gracefully")
	}()
	<-signalChan
	logger.Info("received shutdown signal")
	go func() {
		for {
			quitChan <- struct{}{}
		}
	}()

	shutdownCtx, shutdownRelease := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownRelease()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Fatal("HTTP server shutdown error", zap.Error(err))
	}
	close(requestChan)
	logger.Info("graceful shutdown complete")
}
