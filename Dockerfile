FROM golang:1.15.8 as builder
COPY . /app
WORKDIR /app
RUN CGO_ENABLED=0 GOOS=linux go build ./cmd/kafka-idle-topics/kafka-idle-topics.go

FROM scratch
COPY --from=builder /app/kafka-idle-topics /
ADD ./cmd/trustedEntities/LetsEncryptCA.pem /etc/ssl/certs/LetsEncryptCA.pem
ENTRYPOINT ["/kafka-idle-topics", "-kafkaSecurity plain_tls"]
