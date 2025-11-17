# Ridley

Ridley is a small Go service intended to receive Prometheus `remote_write` traffic from multiple Prometheus replicas in an HA configuration with the identical scrape targets and de-duplicate the metrics by only forwarding from a single replica at a time. The repository includes a sample configuration and a `prometheus/` folder with example Prometheus instances and a Docker Compose setup for local testing.

**Notable files**
- `ridley-config.yaml`: example configuration used by the service.
- `remote_write_handler.go`: code that handles Prometheus remote_write requests.
- `conntracker.go`: connection-tracking helper utilities.
- `prometheus/`: example Prometheus configurations and a `docker-compose.yaml` to run a local demo.

## Quickstart

Prerequisites:
- Go toolchain (1.20+ recommended)
- Docker & Docker Compose (for the Prometheus demo)

Build locally:

```bash
cd $(dirname "$0")
go build -o ridley .
```

Run locally:

```bash
# runs the built binary (reads `ridley-config.yaml` from CWD by default)
./ridley

# or run directly with Go during development
go run ./...
```

Prometheus demo using Docker Compose:

```bash
cd prometheus
docker-compose up --build
```

This will start the example Prometheus instances defined under `prometheus/` and point them at the Ridley endpoint as configured in the example configs.

## Configuration

Edit `ridley-config.yaml` to change listening address/port or other runtime options. The repository includes a simple example; adapt it to your environment as needed.

## Development

- Make code changes in Go files.
- Run `go build` to produce an executable.
- Use `go vet` and `golangci-lint` (optional) for extra checks.

## Contributing

Contributions are welcome. Open issues or PRs with bug fixes, improvements, or suggestions.

## License

MIT License