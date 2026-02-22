FROM --platform=$BUILDPLATFORM golang:1.25.7 AS builder
ARG TARGETARCH
ARG TARGETOS

WORKDIR /app
RUN --mount=type=bind,source=.,target=/app CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /ridley .

FROM scratch
COPY --from=builder /ridley /ridley
ENTRYPOINT ["/ridley"]