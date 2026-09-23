FROM golang:1.24-alpine AS builder

WORKDIR /app

# Copy contracts required by validator
COPY contracts /contracts

# Copy go.mod and go.sum from ingestion-service
COPY services/ingestion-service/go.mod services/ingestion-service/go.sum ./services/ingestion-service/
WORKDIR /app/services/ingestion-service
RUN go mod download

# Copy source code
COPY services/ingestion-service ./

# Build binary
RUN CGO_ENABLED=0 GOOS=linux go build -o /ingestion-service ./cmd/ingestion

FROM alpine:3.20

WORKDIR /app
COPY --from=builder /ingestion-service /app/ingestion-service
COPY contracts /contracts

ENV KAFKA_BROKERS=kafka:9092
ENV CONTRACTS_KAFKA_DIR=/contracts/kafka
ENV HTTP_PORT=8081

EXPOSE 8081

ENTRYPOINT ["/app/ingestion-service"]
