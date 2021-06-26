FROM golang:1.15.8 as builder
COPY . /app
WORKDIR /app
RUN GOOS=linux go build ./cmd/kafka-idle-topics/kafka-idle-topics.go

FROM busybox:1.33.1
COPY --from=builder /app/kafka-idle-topics /
ADD cmd/trustedEntities /etc/ssl/certs/
ENTRYPOINT /kafka-idle-topics -bootstrap-servers $KAFKA_BOOTSTRAP -username $KAFKA_USERNAME -password $KAFKA_PASSWORD
