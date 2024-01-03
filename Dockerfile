FROM golang:1.21.1 as builder
COPY . /app
WORKDIR /app
RUN CGO_ENABLED=0 GOOS=linux go build -o kafka-idle-topics ./cmd/kafka-idle-topics/*

FROM scratch
COPY --from=builder /app/kafka-idle-topics /
ADD cmd/trustedEntities/LetsEncryptCA.pem /etc/ssl/certs/LetsEncryptCA.pem
ENTRYPOINT ["/kafka-idle-topics", "-kafkaSecurity", "plain_tls"]
