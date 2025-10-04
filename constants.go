package main

import "time"

var (
	FLAG_DEFAULT_CHANNEL_LENGTH = 100
	FLAG_DEFAULT_CLIENT_TIMEOUT = 10 * time.Second
	FLAG_DEFAULT_DEBUG          = false
	FLAG_DEFAULT_LISTEN_ADDRESS = "0.0.0.0:8080"
	FLAG_DEFAULT_SWITCH_TIMEOUT = 30 * time.Second
	FLAG_DEFAULT_TARGET         = "http://localhost:9090/api/v1/write"
	FLAG_DEFAULT_TARGET_HEADERS = ""

	FLAG_NAME_CHANNEL_LENGTH = "channel-length"
	FLAG_NAME_CLIENT_TIMEOUT = "client-timeout"
	FLAG_NAME_DEBUG          = "debug"
	FLAG_NAME_LISTEN_ADDRESS = "listen-address"
	FLAG_NAME_SWITCH_TIMEOUT = "switch-timeout"
	FLAG_NAME_TARGET         = "target"
	FLAG_NAME_TARGET_HEADERS = "target-headers"
)
