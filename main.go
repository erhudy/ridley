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
	logger             *zap.Logger
	mapmutex           *sync.Mutex
	connmap            *map[RemoteAddr]*ConnectionMeta
}

type RemoteAddr string
type Status int

const (
	STATUS_UNKNOWN Status = iota // put UNKNOWN as 0 so an empty int won't evaluate to active
	STATUS_ACTIVE
	STATUS_BACKUP
	STATUS_STALE
)

func (rwh *RemoteWriteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rwh.logger.Debug("remote addr", zap.String("ip:port", r.RemoteAddr))
	// rwh.logger.Info("headers", zap.Any("headers", r.Header))
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
	tc, ok := (*rwh.connmap)[ra]
	if !ok {
		rwh.logger.Debug("no entry found", zap.String("remoteAddress", string(ra)))
		var incstatus Status
		if len(*rwh.connmap) < 1 {
			rwh.logger.Debug("connmap empty, setting active")
			incstatus = STATUS_ACTIVE
		} else {
			rwh.logger.Debug("connmap not empty, setting backup")
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
		(*rwh.connmap)[ra] = newConn
		tc = (*rwh.connmap)[ra]
	}
	rwh.mapmutex.Unlock()

	rwh.logger.Debug("working with this connection", zap.String("remoteAddress", string(ra)), zap.Int("status", int(tc.status)))

	if tc.status == STATUS_ACTIVE {
		rwh.logger.Info("passing through request", zap.String("source", r.RemoteAddr))
		req, err := http.NewRequest(http.MethodPost, "http://localhost:9092/api/v1/write", r.Body)
		if err != nil {
			panic(err)
		}
		req.Header = r.Header
		rwh.client.Do(req)
	} else if tc.status == STATUS_BACKUP {
		tc.requestMutex.Lock()
		rwh.logger.Debug("storing request")

		req := &RequestWthTimestamp{
			request:   r,
			timestamp: time.Now(),
		}
		if tc.storedRequests[tc.currentIndex] == nil {
			rwh.logger.Debug("storing request in empty slot", zap.Int("index", tc.currentIndex))
			tc.storedRequests[tc.currentIndex] = req
		} else {
			rwh.logger.Debug("storing request in occupied slot", zap.Int("index", tc.currentIndex))
			if time.Since(tc.storedRequests[tc.currentIndex].timestamp) < rwh.expirationDuration {
				rwh.logger.Debug("request in index has not expired yet, enlarging slice", zap.Int("index", tc.currentIndex))
				oldSliceSize := tc.requestSliceSize
				// replace the slice with a larger one if we are overrunning
				tc.requestSliceSize = tc.requestSliceSize * 2
				newStoredRequests := make([]*RequestWthTimestamp, tc.requestSliceSize, tc.requestSliceSize)
				for i := 0; i < tc.currentIndex; i++ {
					newStoredRequests = append(newStoredRequests, tc.storedRequests[i])
				}
				for i := tc.currentIndex + oldSliceSize; i < tc.requestSliceSize; i++ {
					newStoredRequests[i] = tc.storedRequests[i-oldSliceSize]
				}
				newStoredRequests[tc.currentIndex] = req
				tc.storedRequests = newStoredRequests
			} else {
				rwh.logger.Debug("overwriting request", zap.Int("index", tc.currentIndex))
				tc.storedRequests[tc.currentIndex] = req
			}
		}
		rwh.logger.Debug("incrementing index", zap.Int("index", tc.currentIndex))
		tc.currentIndex += 1
		rwh.logger.Debug("incremented index", zap.Int("index", tc.currentIndex))
		if tc.currentIndex == tc.requestSliceSize {
			rwh.logger.Debug("index wrapped around, resetting")
			tc.currentIndex = 0
		}

		tc.requestMutex.Unlock()
	} else {
		rwh.logger.Info("ignoring request from source", zap.String("source", r.RemoteAddr), zap.Int("status", int(tc.status)))
	}

}

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}

	mutex := sync.Mutex{}
	statusMap := make(map[RemoteAddr]*ConnectionMeta)
	client := http.Client{}

	rwh := RemoteWriteHandler{
		client:             &client,
		expirationDuration: time.Minute,
		logger:             logger,
		mapmutex:           &mutex,
		connmap:            &statusMap,
	}

	mux := http.NewServeMux()
	mux.Handle("/write", &rwh)

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	logger.Info("started on :8080")

	err = server.ListenAndServe()
	logger.Fatal(err.Error())
}
