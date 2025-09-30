package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var logger *zap.Logger
var v *viper.Viper

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
	v = viper.New()
	v.SetEnvPrefix("ridley")
	v.SetConfigName("ridley")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")

	flag.Int(FLAG_NAME_CHANNEL_LENGTH, FLAG_DEFAULT_CHANNEL_LENGTH, "TKTK")
	flag.Duration(FLAG_NAME_CLIENT_TIMEOUT, FLAG_DEFAULT_CLIENT_TIMEOUT, "TKTK")
	flag.String(FLAG_NAME_LISTEN_ADDRESS, FLAG_DEFAULT_LISTEN_ADDRESS, "TKTK")
	flag.Duration(FLAG_NAME_SWITCH_TIMEOUT, FLAG_DEFAULT_SWITCH_TIMEOUT, "TKTK")
	flag.String(FLAG_NAME_TARGET, FLAG_DEFAULT_TARGET, "TKTK")
	flag.String(FLAG_NAME_TARGET_HEADERS, FLAG_DEFAULT_TARGET_HEADERS, "TKTK")

	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()
	v.BindPFlags(pflag.CommandLine)

	var err error
	logger, err = zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	metric_FailedIncomingRequestsTotal.WithLabelValues().Add(0)

	v.AutomaticEnv()
	if err = v.ReadInConfig(); err == nil {
		logger.Info("loaded configuration from disk")
	} else {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			logger.Info("no config file found")
		} else {
			panic(fmt.Errorf("error loading config file: %w", err))
		}
	}

	targetHeadersString := v.GetString(FLAG_NAME_TARGET_HEADERS)
	targetHeadersMap := make(map[string]string)
	afterSplit := strings.Split(targetHeadersString, ",")
	for _, entry := range afterSplit {
		if entry == "" {
			continue
		}
		entryTrimmed := strings.TrimSpace(entry)
		entrySplit := strings.Split(entryTrimmed, "=")
		if len(entrySplit) != 2 {
			panic(fmt.Errorf("unable to process target headers entry: %v", entrySplit))
		}
		k := strings.TrimSpace(strings.TrimRight(strings.TrimLeft(strings.TrimSpace(entrySplit[0]), "'\""), "'\""))
		v := strings.TrimSpace(strings.TrimRight(strings.TrimLeft(strings.TrimSpace(entrySplit[1]), "'\""), "'\""))
		targetHeadersMap[k] = v
	}
	v.Set(FLAG_NAME_TARGET_HEADERS, targetHeadersMap)

	// show the loaded settings but obscure targetHeaders values to mask any tokens
	targetHeaders := v.GetStringMapString(FLAG_NAME_TARGET_HEADERS)
	for k := range targetHeaders {
		targetHeaders[k] = "<<OBSCURED>>"
	}

	loadedSettings := v.AllSettings()
	// AllSettings lowercases the keys so do it lowercased here
	loadedSettings[FLAG_NAME_TARGET_HEADERS] = targetHeaders
	logger.Info("config settings", zap.Any("settings", loadedSettings))
}

func main() {
	client := http.Client{
		Timeout: v.GetDuration(FLAG_NAME_CLIENT_TIMEOUT),
	}
	requestChan := make(chan RequestWithTimestamp, v.GetInt(FLAG_NAME_CHANNEL_LENGTH))
	sendChan := make(chan RequestWithTimestamp, v.GetInt(FLAG_NAME_CHANNEL_LENGTH))
	quitChan := make(chan struct{}, v.GetInt(FLAG_NAME_CHANNEL_LENGTH))

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
		Addr:    v.GetString(FLAG_NAME_LISTEN_ADDRESS),
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
