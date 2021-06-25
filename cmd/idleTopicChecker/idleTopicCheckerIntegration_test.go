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
	composeEnv = testcontainers.NewLocalDockerCompose([]string{"docker-compose.yml"}, "idleTopicChecker")
	composeEnv.WithCommand([]string{"up", "-d"}).Invoke()
	time.Sleep(time.Duration(10) * time.Second) // give services time to set up

	kafkaUrl = "localhost:9092"
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
	time.Sleep(time.Duration(150) * time.Millisecond)
	StopProduction = true

	presentTopics := map[string][]int32{topicA: {0}, topicB: {0}}
	expectedTopicResult := map[string][]int32{topicB: {0}}

	actualResult := filterEmptyTopics(presentTopics, clusterClient)

	assert.Equal(t, expectedTopicResult, actualResult)

	log.Printf("Finished Assessment for no storage, cleaning up...")
	deleteTopicHelper(topicA)
	deleteTopicHelper(topicB)
}


func TestFilterActiveProducerTopics (t *testing.T) {

	createTopicHelper(topicA)
	createTopicHelper(topicB)

	go produceTopicHelper(topicA)
	time.Sleep(time.Duration(150) * time.Millisecond)

	expectedTopicResult := map[string][]int32{topicB: {0}}
	presentTopics := getClusterTopics(adminClient)
	// Delete a pre-set topic
	delete (presentTopics, "_confluent-license")

	_, result := filterActiveProductionTopics(presentTopics, clusterClient)

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

	time.Sleep(time.Duration(150) * time.Millisecond)

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
	err := adminClient.CreateTopic(topicName,&thisTopicDetail, false)
	if err != nil {
		log.Printf("Could not create topic: %v", err)
	}
	log.Printf("Created Topic: %s", topicName)
}

func deleteTopicHelper (topicName string) {
	err := adminClient.DeleteTopic(topicName)
	if err != nil {
		log.Printf("Could not delete topic: %v", err)
	}
	time.Sleep(time.Duration(5) * time.Second)

	for {
		topics, err := adminClient.ListTopics()
		if err != nil {
			log.Printf("Cannot list topics: %v", topics)
		}
		_, exists := topics[topicName]
		if !exists {
			break
		}
	}
	log.Printf("Deleted Topic: %s", topicName)
}

func deleteConsumerGroupHelper (groupName string) {
	err := adminClient.DeleteConsumerGroup(groupName)
	if err != nil {
		log.Printf("Could not delete consumer group: %v", err)
	}
	log.Printf("Deleted Consumer Group: %s", groupName)

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
	log.Printf("In producer helper")

	producer, err := sarama.NewSyncProducerFromClient(clusterClient)
	if err != nil {
		log.Fatalf("Could not produce to test cluster: %v", err)
	}
	log.Printf("Started producer helper")

	for {
		if StopProduction == true {
			producer.Close()
			StopProduction = false
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
		p, o, err := producer.SendMessage(&message)
		if err != nil {
			log.Printf("Cannot produce to cluster: %v", err)
		}
		log.Printf("Produced to partition %v and offset %v", p, o)
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