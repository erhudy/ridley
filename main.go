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
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var logger *zap.SugaredLogger
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
	v.SetConfigName("ridley-config")
	v.SetConfigType("yaml")
	v.AddConfigPath(".")

	flag.Duration(FLAG_NAME_CLIENT_TIMEOUT, FLAG_DEFAULT_CLIENT_TIMEOUT, "Set the timeout of the HTTP client that sends to the target")
	flag.Bool(FLAG_NAME_DEBUG, FLAG_DEFAULT_DEBUG, "Log in debug mode")
	flag.String(FLAG_NAME_LISTEN_ADDRESS, FLAG_DEFAULT_LISTEN_ADDRESS, "Set listen address for Ridley")
	flag.Duration(FLAG_NAME_SWITCH_TIMEOUT, FLAG_DEFAULT_SWITCH_TIMEOUT, "Set timeout after which Ridley will switch to a different stream")
	flag.String(FLAG_NAME_TARGET, FLAG_DEFAULT_TARGET, "Set address of target to send to")
	flag.String(FLAG_NAME_TARGET_HEADERS, FLAG_DEFAULT_TARGET_HEADERS, "Set headers to add to requests being sent to the target in the form \"key1=val1,key2=val2\"")

	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()
	v.BindPFlags(pflag.CommandLine)

	var err error
	rawLogger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	logger = rawLogger.Sugar()
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

	// replace the logger here now that we've gone through all config options and can tell for certain if we should be in debug mode
	if v.GetBool(FLAG_NAME_DEBUG) {
		rawLogger, err = zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
		logger = rawLogger.Sugar()
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
		key := strings.TrimSpace(strings.TrimRight(strings.TrimLeft(strings.TrimSpace(entrySplit[0]), "'\""), "'\""))
		value := strings.TrimSpace(strings.TrimRight(strings.TrimLeft(strings.TrimSpace(entrySplit[1]), "'\""), "'\""))
		targetHeadersMap[key] = value
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
	logger.Infow("config settings", "settings", loadedSettings)
}

func main() {
	client := http.Client{
		Timeout: v.GetDuration(FLAG_NAME_CLIENT_TIMEOUT),
	}

	rwh := RemoteWriteHandler{
		client:      &client,
		connTracker: NewConnTracker(v.GetDuration(FLAG_NAME_SWITCH_TIMEOUT)),
	}

	mux := http.NewServeMux()
	mux.Handle("/write", &rwh)
	mux.Handle("/metrics", promhttp.Handler())
	server := &http.Server{
		Addr:    v.GetString(FLAG_NAME_LISTEN_ADDRESS),
		Handler: mux,
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		logger.Info("starting HTTP server")
		if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("HTTP server error", zap.Error(err))
		}
		logger.Info("shutting down gracefully")
	}()
	<-signalChan

	logger.Info("received shutdown signal")
	shutdownCtx, shutdownRelease := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownRelease()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Fatalw("HTTP server shutdown error", "error", err.Error())
	}
	logger.Info("graceful shutdown complete")
}
