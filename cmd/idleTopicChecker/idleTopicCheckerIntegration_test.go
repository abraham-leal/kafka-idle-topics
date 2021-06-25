package main

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"log"
	"os"
	"sync"
	"testing"
	"time"
)

var composeEnv *testcontainers.LocalDockerCompose
var adminClient sarama.ClusterAdmin
var clusterClient sarama.Client
var StopProduction bool
var StopConsumption bool
var topicA = "hasThings"
var topicB = "doesNotHaveThings"

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}

func setup() {
	composeEnv = testcontainers.NewLocalDockerCompose([]string{"docker-compose.yml"}, "intTests")
	composeEnv.WithCommand([]string{"up", "-d"}).Invoke()
	time.Sleep(time.Duration(10) * time.Second) // give services time to set up

	productionAssessmentTime = 30000
	adminClient = getAdminClient("none")
	clusterClient = getClusterClient("none")
}

func teardown () {
	adminClient.Close()
	clusterClient.Close()
}

func TestFilterNoStorageTopics (t *testing.T) {

	createTopicHelper(topicA)
	createTopicHelper(topicB)

	go produceTopicHelper(topicA)
	time.Sleep(time.Duration(5) * time.Second)
	StopProduction = true

	presentTopics := map[string][]int32{topicA: {0}, topicB: {0}}
	expectedTopicResult := map[string][]int32{topicB: {0}}

	actualResult := filterEmptyTopics(presentTopics, clusterClient)

	assert.Equal(t, expectedTopicResult, actualResult)

	deleteTopicHelper(topicA)
	deleteTopicHelper(topicB)
}


func TestFilterActiveProducerTopics (t *testing.T) {

	createTopicHelper(topicA)
	createTopicHelper(topicB)

	go produceTopicHelper(topicA)
	time.Sleep(time.Duration(5) * time.Second)

	expectedTopicResult := map[string][]int32{topicB: {0}}

	_, result := filterActiveProductionTopics(getClusterTopics(adminClient), clusterClient)

	StopProduction = true

	assert.Equal(t, expectedTopicResult, result)

	deleteTopicHelper(topicA)
	deleteTopicHelper(topicB)
}

func TestFilterActiveConsumerGroupTopics (t *testing.T) {

	createTopicHelper(topicA)
	createTopicHelper(topicB)

	go produceTopicHelper(topicA)
	consumerGroupTopicHelper(topicA, "testingCG")

	time.Sleep(time.Duration(5) * time.Second)

	presentTopics := map[string][]int32{topicA: {0}, topicB: {0}}
	expectedTopicResult := map[string][]int32{topicB: {0}}

	result := filterTopicsWithConsumerGroups(presentTopics, adminClient)

	StopProduction = true
	StopConsumption = true

	assert.Equal(t, expectedTopicResult, result)

	deleteTopicHelper(topicA)
	deleteTopicHelper(topicB)
	deleteConsumerGroupHelper("testingCg")
}

func createTopicHelper (topicName string) {
	thisTopicDetail := sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
		ReplicaAssignment: nil,
		ConfigEntries:     nil,
	}
	adminClient.CreateTopic(topicName,&thisTopicDetail, false)
	log.Printf("Created Topic: %s", topicName)
}

func deleteTopicHelper (topicName string) {
	adminClient.DeleteTopic(topicName)
}

func deleteConsumerGroupHelper (groupName string) {
	adminClient.DeleteConsumerGroup(groupName)
}

func consumerGroupTopicHelper (topicName string, cgName string) {
	consumer := Consumer{
		ready: make(chan bool),
	}

	ctx, cancel := context.WithCancel(context.Background())
	consumerGroup, err := sarama.NewConsumerGroupFromClient(cgName, clusterClient)
	if err != nil {
		log.Fatalf("Could not create Consumer Group: %v", err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := consumerGroup.Consume(ctx, []string{topicName}, &consumer); err != nil {
				log.Panicf("Error from consumer: %v", err)
			}
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()
	<-consumer.ready

	if StopConsumption == true {
		cancel()
		wg.Wait()
		consumerGroup.Close()
	}
}

func produceTopicHelper (topicName string) {
	producer, err := sarama.NewSyncProducerFromClient(clusterClient)
	defer producer.Close()
	if err != nil {
		log.Fatalf("Could not produce to test cluster: %v", err)
	}

	for {
		if StopProduction == true {
			return
		}
		message := sarama.ProducerMessage{
			Topic:     topicName,
			Key:       nil,
			Value:     sarama.StringEncoder("This is a message"),
			Headers:   nil,
			Metadata:  nil,
			Offset:    0,
			Partition: 0,
			Timestamp: time.Time{},
		}
		producer.SendMessage(&message)
	}

}


// Sample consumer to use for testing purposes
type Consumer struct {
	ready chan bool
}

func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		session.MarkMessage(message, "")
	}
	return nil
}