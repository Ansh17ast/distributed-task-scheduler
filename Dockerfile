# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install build dependencies
RUN apk add --no-cache git

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source tree and compile
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/coordinator ./cmd/coordinator
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/worker ./cmd/worker

# Final minimal runtime stage
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app
COPY --from=builder /bin/coordinator /usr/local/bin/coordinator
COPY --from=builder /bin/worker /usr/local/bin/worker

EXPOSE 7001 8001 9001
ENTRYPOINT ["/usr/local/bin/coordinator"]
