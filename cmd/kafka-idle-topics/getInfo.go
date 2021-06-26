package main

import (
	"errors"
	"os"
)

func GetBootstrapServers() string {
	key, present := os.LookupEnv("KAFKA_BOOTSTRAP")
	if present && key != "" {
		return key
	}

	panic(errors.New("KAFKA_BOOTSTRAP environment variable has not been specified"))
}

func GetKafkaUsername() string {
	secret, present := os.LookupEnv("KAFKA_USERNAME")
	if present && secret != "" {
		return secret
	}

	panic(errors.New("KAFKA_USERNAME environment variable has not been specified"))
}

func GetKafkaPassword() string {
	url, present := os.LookupEnv("KAFKA_PASSWORD")
	if present && url != "" {
		return url
	}

	panic(errors.New("KAFKA_PASSWORD environment variable has not been specified"))
}
