package main

import (
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"
)

type RequestWthTimestamp struct {
	request   *http.Request
	timestamp time.Time
}

type ConnectionMeta struct {
	currentIndex     int
	status           Status
	requestMutex     *sync.Mutex
	requestSliceSize int
	storedRequests   []*RequestWthTimestamp
}

type RemoteWriteHandler struct {
	client             *http.Client
	expirationDuration time.Duration
	mapmutex           *sync.Mutex
	connmap            map[RemoteAddr]*ConnectionMeta

	lastRequestFromActive chan<- time.Time
}

type RemoteAddr string
type Status int

var logger *zap.Logger

const (
	STATUS_UNKNOWN Status = iota // put UNKNOWN as 0 so an empty int won't evaluate to active
	STATUS_ACTIVE
	STATUS_BACKUP
	STATUS_STALE
)

func (rwh *RemoteWriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger.Debug("remote addr", zap.String("ip:port", r.RemoteAddr))
	// logger.Info("headers", zap.Any("headers", r.Header))
	/* example headers:
	{
		"Content-Encoding":["snappy"],
		"Content-Length":["13317"],
		"Content-Type":["application/x-protobuf"],
		"User-Agent":["Prometheus/2.54.1"],
		"X-Prometheus-Remote-Write-Version":["0.1.0"]
	}
	*/
	rwh.mapmutex.Lock()
	ra := RemoteAddr(r.RemoteAddr)
	tc, ok := rwh.connmap[ra]
	if !ok {
		logger.Debug("no entry found", zap.String("remoteAddress", string(ra)))
		var incstatus Status
		if len(rwh.connmap) < 1 {
			logger.Debug("connmap empty, setting active")
			incstatus = STATUS_ACTIVE
		} else {
			logger.Debug("connmap not empty, setting backup")
			incstatus = STATUS_BACKUP
		}

		requestSliceSize := 10
		newConn := &ConnectionMeta{
			currentIndex:     0,
			status:           incstatus,
			requestMutex:     &sync.Mutex{},
			requestSliceSize: requestSliceSize,
			storedRequests:   make([]*RequestWthTimestamp, requestSliceSize),
		}
		rwh.connmap[ra] = newConn
		tc = rwh.connmap[ra]
	}
	rwh.mapmutex.Unlock()

	logger.Debug("working with this connection", zap.String("remoteAddress", string(ra)), zap.Int("status", int(tc.status)))

	switch tc.status {
	case STATUS_ACTIVE:
		logger.Info("passing through request", zap.String("source", r.RemoteAddr))
		req, err := http.NewRequest(http.MethodPost, "http://localhost:9092/api/v1/write", r.Body)
		if err != nil {
			panic(err)
		}
		req.Header = r.Header
		rwh.client.Do(req)
		rwh.lastRequestFromActive <- time.Now()
	case STATUS_BACKUP:
		tc.requestMutex.Lock()
		logger.Debug("handling request from backup source")

		req := &RequestWthTimestamp{
			request:   r,
			timestamp: time.Now(),
		}
		if tc.storedRequests[tc.currentIndex] == nil {
			logger.Debug("storing request in empty slot", zap.Int("index", tc.currentIndex))
			tc.storedRequests[tc.currentIndex] = req
		} else {
			logger.Debug("storing request in occupied slot", zap.Int("index", tc.currentIndex))
			if time.Since(tc.storedRequests[tc.currentIndex].timestamp) < rwh.expirationDuration {
				logger.Debug("request in index has not expired yet, enlarging slice", zap.Int("index", tc.currentIndex))
				oldSliceSize := tc.requestSliceSize
				// replace the slice with a larger one if we are overrunning
				tc.requestSliceSize = tc.requestSliceSize * 2
				newStoredRequests := make([]*RequestWthTimestamp, tc.requestSliceSize)
				for i := 0; i < tc.currentIndex; i++ {
					newStoredRequests = append(newStoredRequests, tc.storedRequests[i])
				}
				for i := tc.currentIndex + oldSliceSize; i < tc.requestSliceSize; i++ {
					newStoredRequests[i] = tc.storedRequests[i-oldSliceSize]
				}
				newStoredRequests[tc.currentIndex] = req
				tc.storedRequests = newStoredRequests
			} else {
				logger.Debug("overwriting request", zap.Int("index", tc.currentIndex))
				tc.storedRequests[tc.currentIndex] = req
			}
		}
		logger.Debug("incrementing index", zap.Int("index", tc.currentIndex))
		tc.currentIndex += 1
		logger.Debug("incremented index", zap.Int("index", tc.currentIndex))
		if tc.currentIndex == tc.requestSliceSize {
			logger.Debug("index wrapped around, resetting")
			tc.currentIndex = 0
		}

		tc.requestMutex.Unlock()
	case STATUS_STALE:
		logger.Info("stale connection is back online", zap.String("source", r.RemoteAddr))
		tc.requestMutex.Lock()
		tc.status = STATUS_BACKUP
		tc.requestMutex.Unlock()
	default:
		logger.Info("ignoring request from unrecognized source", zap.String("source", r.RemoteAddr), zap.Int("status", int(tc.status)))
	}
}

func watchdog(mutex *sync.Mutex, statusMap map[RemoteAddr]*ConnectionMeta, expirationDuration time.Duration, lastRequestChan <-chan time.Time, quitChan <-chan struct{}) {
	ticker := time.NewTicker(1 * time.Second)

	var lastRequest time.Time

	go func() {
		for {
			lastRequest = <-lastRequestChan
			logger.Debug("received last request time update")
		}
	}()

	for {
		select {
		case <-ticker.C:
			logger.Info("tick", zap.Int64("lastRequest", lastRequest.Unix()))
			if time.Since(lastRequest) > expirationDuration && len(statusMap) > 0 {
				logger.Info("last request was received longer ago than expiration duration, switching")
				// TODO fix this so it doesn't deadlock
				mutex.Lock()
				defer mutex.Unlock()

				for _, v := range statusMap {
					if v.status == STATUS_ACTIVE {
						v.status = STATUS_STALE
					}
				}

				found := false
				for _, v := range statusMap {
					if v.status == STATUS_BACKUP {
						found = true
						v.status = STATUS_ACTIVE
						break
					}
				}

				if !found {
					logger.Error("no available backup connections")
				}
			}
		case <-quitChan:
			ticker.Stop()
			return
		}
	}
}

func init() {
	var err error
	logger, err = zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
}

func main() {
	mutex := sync.Mutex{}
	statusMap := make(map[RemoteAddr]*ConnectionMeta)
	client := http.Client{}
	lastRequestChan := make(chan time.Time)
	quitChan := make(chan struct{})

	rwh := RemoteWriteHandler{
		client:                &client,
		expirationDuration:    time.Minute,
		mapmutex:              &mutex,
		connmap:               statusMap,
		lastRequestFromActive: lastRequestChan,
	}

	mux := http.NewServeMux()
	mux.Handle("/write", &rwh)

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go watchdog(&mutex, statusMap, rwh.expirationDuration, lastRequestChan, quitChan)
	logger.Info("started on :8080")

	err := server.ListenAndServe()
	quitChan <- struct{}{}
	logger.Fatal(err.Error())
}
