# syntax=docker/dockerfile:1.7

# No GOARCH: the build stage runs as the target platform (QEMU in CI), so the
# binary always matches the image's architecture.
FROM golang:1.25-alpine AS builder
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
# VERSION is reported to core at registration.
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -ldflags "-X main.version=${VERSION}" -o /out/tcp-channel ./cmd/tcp-channel

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app

COPY --from=builder /out/tcp-channel /app/tcp-channel
# The binary is configured entirely from the environment (see
# internal/config.LoadFromEnv).
# No port: the channel only dials out (to core, and to minions that listen).

ENTRYPOINT ["/app/tcp-channel"]
