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
	connmap            map[RemoteAddr]ConnectionMeta

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
	logger.Debug("take lock at position A")
	rwh.mapmutex.Lock()
	logger.Debug("took lock at position A")
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
		newConn := ConnectionMeta{
			currentIndex:     0,
			status:           incstatus,
			requestMutex:     &sync.Mutex{},
			requestSliceSize: requestSliceSize,
			storedRequests:   make([]*RequestWthTimestamp, requestSliceSize),
		}
		rwh.connmap[ra] = newConn
		tc = rwh.connmap[ra]
	}
	logger.Debug("release lock at position A")
	rwh.mapmutex.Unlock()
	logger.Debug("released lock at position A")

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
		logger.Debug("handling request from backup source")

		req := &RequestWthTimestamp{
			request:   r,
			timestamp: time.Now(),
		}
		if tc.storedRequests[tc.currentIndex] == nil {
			logger.Debug("storing request in empty slot", zap.Int("index", tc.currentIndex))
			logger.Debug("take lock at position B")
			tc.requestMutex.Lock()
			logger.Debug("took lock at position B")
			tc.storedRequests[tc.currentIndex] = req
			logger.Debug("release lock at position B")
			tc.requestMutex.Unlock()
			logger.Debug("released lock at position B")
		} else {
			logger.Debug("storing request in occupied slot", zap.Int("index", tc.currentIndex))
			if time.Since(tc.storedRequests[tc.currentIndex].timestamp) < rwh.expirationDuration {
				logger.Debug("request in index has not expired yet, enlarging slice", zap.Int("index", tc.currentIndex))
				logger.Debug("take lock at position C")
				tc.requestMutex.Lock()
				logger.Debug("took lock at position C")
				oldSliceSize := tc.requestSliceSize
				// replace the slice with a larger one if we are overrunning
				newSliceSize := tc.requestSliceSize * 2
				newStoredRequests := make([]*RequestWthTimestamp, newSliceSize)
				for i := 0; i < tc.currentIndex; i++ {
					newStoredRequests = append(newStoredRequests, tc.storedRequests[i])
				}
				for i := tc.currentIndex + oldSliceSize; i < tc.requestSliceSize; i++ {
					newStoredRequests[i] = tc.storedRequests[i-oldSliceSize]
				}
				newStoredRequests[tc.currentIndex] = req
				tc.requestSliceSize = newSliceSize
				tc.storedRequests = newStoredRequests
				logger.Debug("release lock at position C")
				tc.requestMutex.Unlock()
				logger.Debug("released lock at position C")
			} else {
				logger.Debug("overwriting request", zap.Int("index", tc.currentIndex))
				logger.Debug("take lock at position D")
				tc.requestMutex.Lock()
				logger.Debug("took lock at position D")
				tc.storedRequests[tc.currentIndex] = req
				logger.Debug("release lock at position D")
				tc.requestMutex.Unlock()
				logger.Debug("released lock at position D")
			}
		}
		logger.Debug("incrementing index", zap.Int("index", tc.currentIndex))
		logger.Debug("take lock at position E")
		tc.requestMutex.Lock()
		logger.Debug("took lock at position E")
		tc.currentIndex += 1
		logger.Debug("release lock at position E")
		tc.requestMutex.Unlock()
		logger.Debug("released lock at position E")
		logger.Debug("incremented index", zap.Int("index", tc.currentIndex))
		if tc.currentIndex == tc.requestSliceSize {
			logger.Debug("index wrapped around, resetting")
			logger.Debug("take lock at position F")
			tc.requestMutex.Lock()
			logger.Debug("took lock at position F")
			tc.currentIndex = 0
			logger.Debug("release lock at position F")
			tc.requestMutex.Unlock()
			logger.Debug("released lock at position F")
		}

	case STATUS_STALE:
		logger.Info("stale connection is back online", zap.String("source", r.RemoteAddr))
		logger.Debug("take lock at position G")
		tc.requestMutex.Lock()
		logger.Debug("took lock at position G")
		tc.status = STATUS_BACKUP
		logger.Debug("release lock at position G")
		tc.requestMutex.Unlock()
		logger.Debug("released lock at position G")
	default:
		logger.Info("ignoring request from unrecognized source", zap.String("source", r.RemoteAddr), zap.Int("status", int(tc.status)))
	}
}

func watchdog(mapmutex *sync.Mutex, statusMap map[RemoteAddr]ConnectionMeta, expirationDuration time.Duration, lastRequestChan <-chan time.Time, quitChan <-chan struct{}) {
	ticker := time.NewTicker(1 * time.Second)

	var lastRequest time.Time

	go func() {
		for {
			lastRequest = <-lastRequestChan
			logger.Debug("received last request time update")
		}
	}()

	switchingLock := sync.Mutex{}
	timeOfLastSwitch := time.Time{}
	for {
		select {
		case <-ticker.C:
			logger.Info("tick", zap.Int64("lastRequest", lastRequest.Unix()), zap.Int64("lastSwitch", timeOfLastSwitch.Unix()))
			for k, v := range statusMap {
				logger.Debug("status map entry", zap.String("remote addr", string(k)), zap.Int("status", int(v.status)))
			}
			// TODO i need to rewrite this so that it is fully atomic and it doesn't switch both of them to STATUS_STALE
			switchingLock.Lock()
			if time.Since(timeOfLastSwitch) < time.Minute {
				logger.Info("switched within the last minute, waiting to stabilize")
				switchingLock.Unlock()
				break
			}

			if time.Since(lastRequest) > expirationDuration && len(statusMap) > 0 {
				logger.Info("last request was received longer ago than expiration duration, switching")
				logger.Debug("take lock at position H")
				mapmutex.Lock()
				logger.Debug("took lock at position H")

				for k, v := range statusMap {
					if v.status == STATUS_ACTIVE {
						v.status = STATUS_STALE
						statusMap[k] = v
					}
				}

				found := false
				for k, v := range statusMap {
					if v.status == STATUS_BACKUP {
						found = true
						v.status = STATUS_ACTIVE
						statusMap[k] = v
						timeOfLastSwitch = time.Now()
						break
					}
				}

				if !found {
					logger.Error("no available backup connections")
				}
				logger.Debug("release lock at position H")
				mapmutex.Unlock()
				logger.Debug("released lock at position H")
			}
			switchingLock.Unlock()
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
	mapmutex := sync.Mutex{}
	statusMap := make(map[RemoteAddr]ConnectionMeta)
	client := http.Client{}
	lastRequestChan := make(chan time.Time)
	quitChan := make(chan struct{})

	rwh := RemoteWriteHandler{
		client:                &client,
		expirationDuration:    time.Minute,
		mapmutex:              &mapmutex,
		connmap:               statusMap,
		lastRequestFromActive: lastRequestChan,
	}

	mux := http.NewServeMux()
	mux.Handle("/write", &rwh)

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go watchdog(&mapmutex, statusMap, rwh.expirationDuration, lastRequestChan, quitChan)
	logger.Info("started on :8080")

	err := server.ListenAndServe()
	quitChan <- struct{}{}
	logger.Fatal(err.Error())
}
