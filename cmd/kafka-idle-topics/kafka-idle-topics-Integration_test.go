package main

import (
	"context"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
)

var composeEnv *testcontainers.LocalDockerCompose
var adminClient sarama.ClusterAdmin
var clusterClient sarama.Client
var StopProduction = false
var StopConsumption = false
var topicA = "hasThings"
var topicB = "doesNotHaveThings"
var instanceOfChecker = NewKafkaIdleTopics()

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}

func setup() {
	composeEnv = testcontainers.NewLocalDockerCompose([]string{"docker-compose.yml"}, "kafka-idle-topics")
	composeEnv.WithCommand([]string{"up", "-d"}).Invoke()
	time.Sleep(time.Duration(10) * time.Second) // give services time to set up

	instanceOfChecker.kafkaUrl = "localhost:9092"
	instanceOfChecker.productionAssessmentTime = 30000
	adminClient = instanceOfChecker.getAdminClient("none")
	clusterClient = instanceOfChecker.getClusterClient("none")
}

func teardown() {
	adminClient.Close()
	clusterClient.Close()
}

func TestFilterNoStorageTopics(t *testing.T) {
	instanceOfChecker.DeleteCandidates = map[string]bool{}
	StopProduction = false
	StopConsumption = false

	createTopicHelper(topicA)
	createTopicHelper(topicB)

	go produceTopicHelper(topicA)
	time.Sleep(time.Duration(150) * time.Millisecond)
	StopProduction = true

	instanceOfChecker.topicPartitionMap = map[string][]int32{topicA: {0}, topicB: {0}}
	expectedTopicResult := map[string]bool{topicB: true}

	instanceOfChecker.filterEmptyTopics(clusterClient)
	instanceOfChecker.filterOutDeleteCandidates()

	assert.Equal(t, expectedTopicResult, instanceOfChecker.DeleteCandidates)

	log.Printf("Finished Assessment for no storage, cleaning up...")
	deleteTopicHelper(topicA)
	deleteTopicHelper(topicB)
}

func TestFilterActiveProducerTopics(t *testing.T) {
	instanceOfChecker.DeleteCandidates = map[string]bool{}
	StopProduction = false
	StopConsumption = false

	createTopicHelper(topicA)
	createTopicHelper(topicB)

	go produceTopicHelper(topicA)
	time.Sleep(time.Duration(150) * time.Millisecond)

	expectedTopicResult := map[string]bool{topicB: true}
	instanceOfChecker.topicPartitionMap = map[string][]int32{topicA: {0}, topicB: {0}}

	instanceOfChecker.filterActiveProductionTopics(clusterClient)
	instanceOfChecker.filterOutDeleteCandidates()

	StopProduction = true

	assert.Equal(t, expectedTopicResult, instanceOfChecker.DeleteCandidates)

	deleteTopicHelper(topicA)
	deleteTopicHelper(topicB)
}

func TestFilterActiveConsumerGroupTopics(t *testing.T) {
	instanceOfChecker.DeleteCandidates = map[string]bool{}
	StopProduction = false
	StopConsumption = false

	createTopicHelper(topicA)
	createTopicHelper(topicB)

	go produceTopicHelper(topicA)
	go consumerGroupTopicHelper(topicA, "testingCG")

	time.Sleep(time.Duration(10) * time.Second)

	instanceOfChecker.topicPartitionMap = map[string][]int32{topicA: {0}, topicB: {0}}
	expectedTopicResult := map[string]bool{topicB: true}

	instanceOfChecker.filterTopicsWithConsumerGroups(adminClient)
	instanceOfChecker.filterOutDeleteCandidates()

	StopProduction = true
	StopConsumption = true
	time.Sleep(time.Duration(100) * time.Millisecond)

	assert.Equal(t, expectedTopicResult, instanceOfChecker.DeleteCandidates)

	deleteTopicHelper(topicA)
	deleteTopicHelper(topicB)
}

func TestCandidacyRemoval(t *testing.T) {
	instanceOfChecker.DeleteCandidates = map[string]bool{}
	StopProduction = false
	StopConsumption = false

	createTopicHelper(topicA)
	createTopicHelper(topicB)

	go produceTopicHelper(topicA)
	time.Sleep(time.Duration(150) * time.Millisecond)
	StopProduction = true

	instanceOfChecker.topicPartitionMap = map[string][]int32{topicA: {0}, topicB: {0}}
	expectedTopicResult := map[string]bool{}

	// topicA is no longer empty, which means it is not a delete candidate
	instanceOfChecker.filterEmptyTopics(clusterClient)

	// At this point, topicB is a candidate, but we'll remove candidacy due to active consumer groups
	go produceTopicHelper(topicB)
	go consumerGroupTopicHelper(topicB, "testingCG")
	time.Sleep(time.Duration(10) * time.Second)

	instanceOfChecker.filterTopicsWithConsumerGroups(adminClient)

	StopProduction = true
	StopConsumption = true

	time.Sleep(time.Duration(100) * time.Millisecond)

	instanceOfChecker.filterOutDeleteCandidates()

	// There should be no candidates for deletion
	assert.Equal(t, expectedTopicResult, instanceOfChecker.DeleteCandidates)

	log.Printf("Finished Assessment for candidacy, cleaning up...")
	deleteTopicHelper(topicA)
	deleteTopicHelper(topicB)
}


func createTopicHelper(topicName string) {
	thisTopicDetail := sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
		ReplicaAssignment: nil,
		ConfigEntries:     nil,
	}
	err := adminClient.CreateTopic(topicName, &thisTopicDetail, false)
	if err != nil {
		log.Printf("Could not create topic: %v", err)
	}
	log.Printf("Created Topic: %s", topicName)
}

func deleteTopicHelper(topicName string) {
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

func consumerGroupTopicHelper(topicName string, cgName string) {
	consumer := Consumer{
		ready: make(chan bool),
	}

	log.Println("Starting Consumer")
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

	for {
		if StopConsumption == true {
			log.Println("Ending consumer")
			break
		}
	}
	cancel()
	wg.Wait()
	err = consumerGroup.Close()
	if err != nil {
		log.Printf("Could not end consumer group: %v", err)
	}
}

func produceTopicHelper(topicName string) {
	producer, err := sarama.NewSyncProducerFromClient(clusterClient)
	if err != nil {
		log.Fatalf("Could not produce to test cluster: %v", err)
	}

	log.Println("Starting Producer")
	for {
		if StopProduction == true {
			producer.Close()
			log.Printf("Ending producer")
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
		_, _, err := producer.SendMessage(&message)
		if err != nil {
			log.Printf("Cannot produce to cluster: %v", err)
		}
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
