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
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var logger *zap.SugaredLogger
var v *viper.Viper

type RidleyMetrics struct {
	FailedIncomingRequestsTotal *prometheus.CounterVec
	IncomingRequestsTotal       *prometheus.CounterVec
	SendErrorsTotal             *prometheus.CounterVec
}

func NewRidleyMetrics(reg prometheus.Registerer) *RidleyMetrics {
	m := &RidleyMetrics{
		FailedIncomingRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: "ridley_failed_incoming_requests_total"}, []string{}),
		IncomingRequestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: "ridley_incoming_requests_total"}, []string{"replica"}),
		SendErrorsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: "ridley_send_errors_total"}, []string{"code"}),
	}
	reg.MustRegister(m.FailedIncomingRequestsTotal, m.IncomingRequestsTotal, m.SendErrorsTotal)
	m.FailedIncomingRequestsTotal.WithLabelValues().Add(0)
	return m
}

func parseTargetHeaders(raw string) (map[string]string, error) {
	result := make(map[string]string)
	for _, entry := range strings.Split(raw, ",") {
		if entry == "" {
			continue
		}
		parts := strings.Split(strings.TrimSpace(entry), "=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("unable to process target headers entry: %v", parts)
		}
		key := strings.TrimSpace(strings.Trim(strings.TrimSpace(parts[0]), "'\""))
		value := strings.TrimSpace(strings.Trim(strings.TrimSpace(parts[1]), "'\""))
		result[key] = value
	}
	return result, nil
}

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
}

func main() {
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()
	if err := v.BindPFlags(pflag.CommandLine); err != nil {
		panic(fmt.Errorf("failed to bind pflags: %w", err))
	}

	var err error
	rawLogger, err := zap.NewProduction()
	if err != nil {
		panic(err)
	}
	logger = rawLogger.Sugar()

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

	targetHeadersMap, err := parseTargetHeaders(v.GetString(FLAG_NAME_TARGET_HEADERS))
	if err != nil {
		panic(err)
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

	client := http.Client{
		Timeout: v.GetDuration(FLAG_NAME_CLIENT_TIMEOUT),
	}

	metrics := NewRidleyMetrics(prometheus.DefaultRegisterer)
	rwh := RemoteWriteHandler{
		client:      &client,
		connTracker: NewConnTracker(v.GetDuration(FLAG_NAME_SWITCH_TIMEOUT), logger),
		target:      v.GetString(FLAG_NAME_TARGET),
		addlHeaders: v.GetStringMapString(FLAG_NAME_TARGET_HEADERS),
		logger:      logger,
		metrics:     metrics,
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
