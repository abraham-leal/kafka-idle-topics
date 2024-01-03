FROM golang:1.21.1 as builder
COPY . /app
WORKDIR /app/kafka-idle-topics
RUN CGO_ENABLED=0 GOOS=linux go build

FROM scratch
COPY --from=builder /app/kafka-idle-topics /
ADD trustedEntities/LetsEncryptCA.pem /etc/ssl/certs/LetsEncryptCA.pem
ENTRYPOINT ["/kafka-idle-topics", "-kafkaSecurity plain_tls"]
