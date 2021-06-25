FROM golang:1.15.8 as builder
COPY cmd /app
WORKDIR /app
RUN go get github.com/Shopify/sarama
RUN GOOS=linux go build ./cmd/idleTopicChecker/idleTopicChecker.go

FROM scratch
COPY --from=builder /app/idleTopicChecker /
ADD cmd/trustedEntities /etc/ssl/certs/
ENTRYPOINT ["/idleTopicChecker", "-bootstrap-servers", "$KAFKA_BOOTSRAP", "-username", "$KAFKA_USERNAME","-password", "$KAFKA_PASSWORD"]