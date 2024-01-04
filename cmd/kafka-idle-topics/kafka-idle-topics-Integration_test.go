package main

import (
	"context"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
)

var StopProduction = false
var StopConsumption = false
var topicA = "hasThings"
var topicB = "doesNotHaveThings"
var instanceOfChecker = NewKafkaIdleTopics()
var ctx = context.Background()
var kafkaContainer = kafka.KafkaContainer{}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	teardown()
	os.Exit(code)
}

func setup() {

	kafkaContainer, err := kafka.RunContainer(ctx,
		kafka.WithClusterID("test-cluster"),
		testcontainers.WithImage("confluentinc/confluent-local:7.5.3"),
	)
	if err != nil {
		panic(err)
	}

	brokerList, err := kafkaContainer.Brokers(ctx)
	if err != nil {
		log.Fatal("Kafka was not able to start")
	}

	instanceOfChecker.kafkaUrl = brokerList[0]
	instanceOfChecker.productionAssessmentTime = 30000
}

func teardown() {
	log.Println("Ended tests!")
}

func TestFilterAllowListTopics(t *testing.T) {
	log.Printf("Starting Assessment for Allowlist")
	AllowList = StringArrayFlag{topicB: true}
	DisallowList = nil
	adminClient := instanceOfChecker.getAdminClient("none")
	defer adminClient.Close()

	createTopicHelper(topicA)
	createTopicHelper(topicB)

	actualTopics := instanceOfChecker.getClusterTopics(adminClient)

	expectedTopicResult := map[string][]int32{topicB: {0}}

	assert.Equal(t, expectedTopicResult, actualTopics)

	log.Printf("Finished Assessment for Allowlist, cleaning up...")
	AllowList = nil
	DisallowList = nil
	deleteTopicHelper(topicA)
	deleteTopicHelper(topicB)
}

func TestFilterDisAllowListTopics(t *testing.T) {
	log.Printf("Starting Assessment for Disallowlist")
	DisallowList = StringArrayFlag{topicA: true}
	AllowList = nil
	adminClient := instanceOfChecker.getAdminClient("none")
	defer adminClient.Close()

	createTopicHelper(topicA)
	createTopicHelper(topicB)

	actualTopics := instanceOfChecker.getClusterTopics(adminClient)

	expectedTopicResult := map[string][]int32{topicB: {0}}

	assert.Equal(t, expectedTopicResult, actualTopics)

	log.Printf("Finished Assessment for Disallowlist, cleaning up...")
	DisallowList = nil
	AllowList = nil
	deleteTopicHelper(topicA)
	deleteTopicHelper(topicB)
}

func TestFilterNoStorageTopics(t *testing.T) {
	log.Printf("Starting Assessment for No Storage")
	instanceOfChecker.DeleteCandidates = map[string]bool{}
	StopProduction = false
	StopConsumption = false
	clusterClient := instanceOfChecker.getClusterClient("none")
	defer clusterClient.Close()

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
	log.Printf("Starting Assessment for Active Producing")
	instanceOfChecker.DeleteCandidates = map[string]bool{}
	StopProduction = false
	StopConsumption = false
	clusterClient := instanceOfChecker.getClusterClient("none")
	defer clusterClient.Close()

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

	log.Printf("Finished Assessment for active producing, cleaning up...")
	deleteTopicHelper(topicA)
	deleteTopicHelper(topicB)
}

func TestFilterActiveConsumerGroupTopics(t *testing.T) {
	log.Printf("Starting Assessment for Active CGs")
	instanceOfChecker.DeleteCandidates = map[string]bool{}
	StopProduction = false
	StopConsumption = false
	adminClient := instanceOfChecker.getAdminClient("none")
	defer adminClient.Close()

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
	time.Sleep(time.Duration(500) * time.Millisecond)

	assert.Equal(t, expectedTopicResult, instanceOfChecker.DeleteCandidates)

	log.Printf("Finished Assessment for active CGs, cleaning up...")
	deleteTopicHelper(topicA)
	deleteTopicHelper(topicB)
}

func TestCandidacyRemoval(t *testing.T) {
	log.Printf("Starting Assessment for Candidacy Removal")
	instanceOfChecker.DeleteCandidates = map[string]bool{}
	StopProduction = false
	StopConsumption = false
	clusterClient := instanceOfChecker.getClusterClient("none")
	defer clusterClient.Close()
	adminClient := instanceOfChecker.getAdminClient("none")
	defer adminClient.Close()

	createTopicHelper(topicA)
	createTopicHelper(topicB)

	go produceTopicHelper(topicA)
	time.Sleep(time.Duration(500) * time.Millisecond)
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

	time.Sleep(time.Duration(500) * time.Millisecond)

	instanceOfChecker.filterOutDeleteCandidates()

	// There should be no candidates for deletion
	assert.Equal(t, expectedTopicResult, instanceOfChecker.DeleteCandidates)

	log.Printf("Finished Assessment for candidacy, cleaning up...")
	deleteTopicHelper(topicA)
	deleteTopicHelper(topicB)
}

func createTopicHelper(topicName string) {
	adminClient := instanceOfChecker.getAdminClient("none")
	defer adminClient.Close()

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

	for {
		td, err := adminClient.ListTopics()
		if err != nil {
			log.Printf("Could not verify topic creation: %v", err)
		}

		_, exists := td[topicName]
		if exists {
			break
		} else {
			log.Println("Topic not created yet " + topicName)
		}
	}

	log.Printf("Created Topic: %s", topicName)
}

func deleteTopicHelper(topicName string) {
	adminClient := instanceOfChecker.getAdminClient("none")
	defer adminClient.Close()

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
	clusterClient := instanceOfChecker.getClusterClient("none")
	defer clusterClient.Close()

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
	clusterClient := instanceOfChecker.getClusterClient("none")
	defer clusterClient.Close()
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
