package main

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type mockHTTPClient struct {
	doFunc func(*http.Request) (*http.Response, error)
}

func (m *mockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return m.doFunc(req)
}

func newTestHandler(client HTTPDoer) *RemoteWriteHandler {
	return &RemoteWriteHandler{
		client:      client,
		connTracker: NewConnTracker(30*time.Second, zap.NewNop().Sugar()),
		target:      "http://upstream.test/write",
		addlHeaders: map[string]string{},
		logger:      zap.NewNop().Sugar(),
		metrics:     NewRidleyMetrics(prometheus.NewRegistry()),
	}
}

type errorReader struct{ err error }

func (e *errorReader) Read(p []byte) (int, error) { return 0, e.err }

func okResponse() *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(strings.NewReader("")),
	}
}

func TestServeHTTP_MissingReplicaHeader_Returns400(t *testing.T) {
	called := false
	client := &mockHTTPClient{doFunc: func(req *http.Request) (*http.Response, error) {
		called = true
		return okResponse(), nil
	}}
	handler := newTestHandler(client)

	req := httptest.NewRequest(http.MethodPost, "/write", strings.NewReader("body"))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.False(t, called)
}

func TestServeHTTP_BodyReadError_Returns500(t *testing.T) {
	called := false
	client := &mockHTTPClient{doFunc: func(req *http.Request) (*http.Response, error) {
		called = true
		return okResponse(), nil
	}}
	handler := newTestHandler(client)

	req := httptest.NewRequest(http.MethodPost, "/write", &errorReader{err: errors.New("read error")})
	req.Header.Set(HEADER_X_RIDLEY_REPLICA, "replica-1")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.False(t, called)
}

func TestServeHTTP_HappyPath_ForwardsToHandler(t *testing.T) {
	called := false
	client := &mockHTTPClient{doFunc: func(req *http.Request) (*http.Response, error) {
		called = true
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("ok")),
		}, nil
	}}
	handler := newTestHandler(client)

	req := httptest.NewRequest(http.MethodPost, "/write", strings.NewReader("body"))
	req.Header.Set(HEADER_X_RIDLEY_REPLICA, "replica-1")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.True(t, called)
}

func TestHandleRequest_ActiveReplica_ForwardsRequest(t *testing.T) {
	client := &mockHTTPClient{doFunc: func(req *http.Request) (*http.Response, error) {
		assert.Equal(t, "http://upstream.test/write", req.URL.String())
		return okResponse(), nil
	}}
	handler := newTestHandler(client)

	w := httptest.NewRecorder()
	handler.HandleRequest(w, http.Header{}, []byte("body"), time.Now(), "replica-1")

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHandleRequest_InactiveReplica_Returns202(t *testing.T) {
	callCount := 0
	client := &mockHTTPClient{doFunc: func(req *http.Request) (*http.Response, error) {
		callCount++
		return okResponse(), nil
	}}
	handler := newTestHandler(client)
	now := time.Now()

	// First request from replica-1 → sets it as active, upstream called
	w1 := httptest.NewRecorder()
	handler.HandleRequest(w1, http.Header{}, []byte("body"), now, "replica-1")
	assert.Equal(t, http.StatusOK, w1.Code)
	assert.Equal(t, 1, callCount)

	// Second request from replica-2 within timeout → 202, upstream not called again
	w2 := httptest.NewRecorder()
	handler.HandleRequest(w2, http.Header{}, []byte("body"), now.Add(5*time.Second), "replica-2")
	assert.Equal(t, http.StatusAccepted, w2.Code)
	assert.Equal(t, 1, callCount)
}

func TestHandleRequest_AdditionalHeadersForwarded(t *testing.T) {
	var capturedReq *http.Request
	client := &mockHTTPClient{doFunc: func(req *http.Request) (*http.Response, error) {
		capturedReq = req
		return okResponse(), nil
	}}
	handler := newTestHandler(client)
	handler.addlHeaders = map[string]string{"X-Custom-Header": "custom-value"}

	w := httptest.NewRecorder()
	handler.HandleRequest(w, http.Header{}, []byte("body"), time.Now(), "replica-1")

	require.NotNil(t, capturedReq)
	assert.Equal(t, "custom-value", capturedReq.Header.Get("X-Custom-Header"))
}

func TestHandleRequest_UpstreamError_Returns500(t *testing.T) {
	client := &mockHTTPClient{doFunc: func(req *http.Request) (*http.Response, error) {
		return nil, errors.New("connection refused")
	}}
	handler := newTestHandler(client)

	w := httptest.NewRecorder()
	handler.HandleRequest(w, http.Header{}, []byte("body"), time.Now(), "replica-1")

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleRequest_BadTargetURL_Returns500(t *testing.T) {
	called := false
	client := &mockHTTPClient{doFunc: func(req *http.Request) (*http.Response, error) {
		called = true
		return nil, nil
	}}
	handler := newTestHandler(client)
	handler.target = "://invalid-url"

	w := httptest.NewRecorder()
	handler.HandleRequest(w, http.Header{}, []byte("body"), time.Now(), "replica-1")

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.False(t, called)
}

func TestHandleRequest_UpstreamResponseBodyPropagated(t *testing.T) {
	client := &mockHTTPClient{doFunc: func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("response body")),
		}, nil
	}}
	handler := newTestHandler(client)

	w := httptest.NewRecorder()
	handler.HandleRequest(w, http.Header{}, []byte("body"), time.Now(), "replica-1")

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "response body", w.Body.String())
}

func TestHandleRequest_UpstreamNon2xx_Propagated(t *testing.T) {
	client := &mockHTTPClient{doFunc: func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusServiceUnavailable,
			Body:       io.NopCloser(strings.NewReader("service unavailable")),
		}, nil
	}}
	handler := newTestHandler(client)

	w := httptest.NewRecorder()
	handler.HandleRequest(w, http.Header{}, []byte("body"), time.Now(), "replica-1")

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Equal(t, "service unavailable", w.Body.String())
}
