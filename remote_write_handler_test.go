package main

import (
    "context"
    "net/http"
    "net/http/httptest"
    "testing"
    "time"
    "sync"
)

func TestDispatchForwardsToClient(t *testing.T) {
    recv := make(chan *http.Request, 1)
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        select {
        case recv <- r:
        default:
        }
        w.WriteHeader(http.StatusOK)
    }))
    defer server.Close()

    // ensure the target points to our test server
    v.Set(FLAG_NAME_TARGET, server.URL)

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    rwh := RemoteWriteHandler{
        client: &http.Client{Timeout: 2 * time.Second},
        connTracker: &ConnTracker{
            activeLastRequestTimestamp: time.Time{},
            conntrackTable:             make(map[string]chan RequestWithTimestamp),
            mutex:                      sync.Mutex{},
        },
        ctx:          ctx,
        requestChan:  make(chan RequestWithTimestamp, 10),
        sendChan:     make(chan RequestWithTimestamp, 10),
        dispatchDone: make(chan struct{}),
    }

    go rwh.Dispatch()

    headers := make(http.Header)
    headers.Set(HEADER_X_RIDLEY_REPLICA, "replica-test-1")
    rwh.requestChan <- RequestWithTimestamp{requestBody: []byte("payload"), requestHeaders: headers, timestamp: time.Now()}

    select {
    case req := <-recv:
        if req.Method != http.MethodPost {
            t.Fatalf("expected POST, got %s", req.Method)
        }
        if req.Header.Get(HEADER_X_RIDLEY_REPLICA) != "replica-test-1" {
            t.Fatalf("expected replica header forwarded, got %s", req.Header.Get(HEADER_X_RIDLEY_REPLICA))
        }
    case <-time.After(2 * time.Second):
        t.Fatalf("timeout waiting for forwarded request")
    }

    // shutdown
    cancel()
    <-rwh.dispatchDone
}

func TestProcessReplicaSwitchOnTimeout(t *testing.T) {
    recv := make(chan *http.Request, 1)
    server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        select {
        case recv <- r:
        default:
        }
        w.WriteHeader(http.StatusOK)
    }))
    defer server.Close()

    v.Set(FLAG_NAME_TARGET, server.URL)
    // set a short switch timeout for the test
    v.Set(FLAG_NAME_SWITCH_TIMEOUT, 100*time.Millisecond)

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    ct := &ConnTracker{
        // set active to replica-old and last request timestamp far in the past
        activeConnection:           "replica-old",
        activeLastRequestTimestamp: time.Now().Add(-time.Hour),
        conntrackTable:             make(map[string]chan RequestWithTimestamp),
        mutex:                      sync.Mutex{},
    }

    rwh := RemoteWriteHandler{
        client:      &http.Client{Timeout: 2 * time.Second},
        connTracker: ct,
        ctx:         ctx,
        requestChan: make(chan RequestWithTimestamp, 10),
        sendChan:    make(chan RequestWithTimestamp, 10),
        dispatchDone: make(chan struct{}),
    }

    go rwh.Dispatch()

    headers := make(http.Header)
    headers.Set(HEADER_X_RIDLEY_REPLICA, "replica-new")
    rwh.requestChan <- RequestWithTimestamp{requestBody: []byte("payload"), requestHeaders: headers, timestamp: time.Now()}

    select {
    case req := <-recv:
        if req.Header.Get(HEADER_X_RIDLEY_REPLICA) != "replica-new" {
            t.Fatalf("expected replica-new to be forwarded, got %s", req.Header.Get(HEADER_X_RIDLEY_REPLICA))
        }
    case <-time.After(2 * time.Second):
        t.Fatalf("timeout waiting for forwarded request after switch")
    }

    cancel()
    <-rwh.dispatchDone
}
