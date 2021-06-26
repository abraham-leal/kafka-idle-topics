package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

var kafkaUrl string
var kafkaUsername string
var kafkaPassword string
var kafkaSecurity string
var fileName string
var productionAssessmentTime int

func GetFlags() {
	flag.StringVar(&kafkaUrl, "bootstrap-servers", "", "Address to the target Kafka Cluster. Accepts multiple endpoints separated by a comma")
	flag.StringVar(&kafkaUsername, "username", "", "Username in the PLAIN module")
	flag.StringVar(&kafkaPassword, "password", "", "Password in the PLAIN module")
	flag.StringVar(&kafkaSecurity, "kafkaSecurity", "plain_tls", "Type of connection to attempt. Options: plain_tls, plain (no tls), tls (one-way), none.")
	flag.StringVar(&fileName, "filename", "idleTopics.txt", "Custom filename for the output if needed.")
	flag.IntVar(&productionAssessmentTime, "productionAssessmentTimeMs", 30000, "Timeframe to assess active production")
	flag.Parse()
}

func main() {
	GetFlags()

	// If the parameters are empty, go fetch from env
	if kafkaUrl == "" || kafkaUsername == "" || kafkaPassword == "" {
		kafkaUrl = GetOSEnvVar("KAFKA_BOOTSTRAP")
		kafkaUsername = GetOSEnvVar("KAFKA_USERNAME")
		kafkaPassword = GetOSEnvVar("KAFKA_PASSWORD")
	}

	adminClient := getAdminClient(kafkaSecurity)
	clusterClient := getClusterClient(kafkaSecurity)
	defer adminClient.Close()
	defer clusterClient.Close()

	// Extract Topics in Cluster
	clusterTopics := getClusterTopics(adminClient)

	_, topicPartitionMap := filterActiveProductionTopics(clusterTopics, clusterClient)

	topicPartitionMap = filterTopicsWithConsumerGroups(topicPartitionMap, adminClient)

	topicPartitionMap = filterEmptyTopics(topicPartitionMap, clusterClient)

	path := writeTopicsLocally(topicPartitionMap)

	log.Printf("Done! A list of found idle topics is available at: %s", path)

}

/*
	Uses the provided Sarama Admin Client to get a list of current topics in the cluster
*/
func getClusterTopics(adminClient sarama.ClusterAdmin) map[string]sarama.TopicDetail {
	log.Println("Loading Topics...")
	topicMetadata, err := adminClient.ListTopics()
	if err != nil {
		log.Fatalf("Could not reach cluster within the last 30 seconds. Is the configuration correct? %v", err)
	}
	return topicMetadata
}

/*
	Takes a list of topics and filters it out of topics that DO have data in them.
	Returns and accepts a List of the form map[string]sarama.TopicDetail.
	The returned list is topics without data in them.
*/
func filterEmptyTopics(topicPartitionMap map[string][]int32, clusterClient sarama.Client) map[string][]int32 {
	log.Println("Evaluating Topics without anything in them...")

	for t, td := range topicPartitionMap {
		thisTopicsPartitions := td
		for _, partition := range thisTopicsPartitions {
			oldestOffsetForPartition, err := clusterClient.GetOffset(t, partition, sarama.OffsetOldest)
			if err != nil {
				log.Fatalf("Could not determine topic storage: %v", err)
			}
			newestOffsetForPartition, err := clusterClient.GetOffset(t, partition, sarama.OffsetNewest)
			if err != nil {
				log.Fatalf("Could not determine topic storage: %v", err)
			}
			if oldestOffsetForPartition != newestOffsetForPartition {
				delete(topicPartitionMap, t)
			}
		}
	}

	return topicPartitionMap
}

/*
	Takes a list of topics and filters it out of topics that are being actively produced to.
	Returns and accepts a List of the form map[string]sarama.TopicDetail.
	The returned list are topics that do not have active producers.
*/
func filterActiveProductionTopics(topicMetadata map[string]sarama.TopicDetail, clusterClient sarama.Client) (map[string]sarama.TopicDetail, map[string][]int32) {
	log.Println("Evaluating Topics without any active production...")

	topicPartitionMap := map[string][]int32{}
	for t, td := range topicMetadata {
		topicPartitionMap[t] = makeRange(0, td.NumPartitions-1)
	}

	beginTopicInspection := map[string]map[int64]int64{}

	for t := range topicMetadata {
		thisTopicCounts := map[int64]int64{}
		for _, partition := range topicPartitionMap[t] {
			newestOffsetForPartition, err := clusterClient.GetOffset(t, partition, sarama.OffsetNewest)
			if err != nil {
				log.Fatalf("Could not determine topic end offset: %v", err)
			}
			thisTopicCounts[int64(partition)] = newestOffsetForPartition
		}
		beginTopicInspection[t] = thisTopicCounts
	}

	// Sleep for configurable time to see if the offsets grow
	log.Printf("Waiting for %v ms to evaluate active production.", productionAssessmentTime)
	time.Sleep(time.Duration(productionAssessmentTime) * time.Millisecond)

	endTopicInspection := map[string]map[int64]int64{}

	for t := range topicMetadata {
		thisTopicCounts := map[int64]int64{}
		for _, partition := range topicPartitionMap[t] {
			newestOffsetForPartition, err := clusterClient.GetOffset(t, partition, sarama.OffsetNewest)
			if err != nil {
				log.Fatalf("Could not determine topic end offset: %v", err)
			}
			thisTopicCounts[int64(partition)] = newestOffsetForPartition
		}
		endTopicInspection[t] = thisTopicCounts
	}

	for topic, partitionOffset := range endTopicInspection {
		if !reflect.DeepEqual(beginTopicInspection[topic], partitionOffset) {
			delete(topicMetadata, topic)
			delete(topicPartitionMap, topic)
		}
	}

	return topicMetadata, topicPartitionMap
}

/*
	Takes a list of topics and filters it out of topics that have active consumer groups. (Existing Offsets)
	Returns a List of the form map[string]sarama.TopicDetail
	The returned list are topics that do not have a consumer group record.
*/
func filterTopicsWithConsumerGroups(topics map[string][]int32, adminClient sarama.ClusterAdmin) map[string][]int32 {
	log.Println("Evaluating Topics without active Consumer Groups...")
	allConsumerGroups, err := adminClient.ListConsumerGroups()
	if err != nil {
		log.Fatalf("Could not obtain Consumer Groups from cluster: %v", err)
	}

	for cg := range allConsumerGroups {
		result, err := adminClient.ListConsumerGroupOffsets(cg, topics)
		if err != nil {
			log.Fatalf("Cannot determine if topic is in use by consumers.")
		}

		for topic, partitionData := range result.Blocks {
			for _, dataset := range partitionData {
				if dataset.Offset != -1 {
					delete(topics, topic)
					break
				}
			}
		}
	}

	return topics
}

func getAdminClient(securityContext string) sarama.ClusterAdmin {
	adminClient, err := sarama.NewClusterAdmin(strings.Split(kafkaUrl, ","), generateClientConfigs(securityContext))
	if err != nil {
		log.Fatalf("Unable to create Kafka Client: %v", err)
	}
	return adminClient
}

func getClusterClient(securityContext string) sarama.Client {
	clusterClient, err := sarama.NewClient(strings.Split(kafkaUrl, ","), generateClientConfigs(securityContext))
	if err != nil {
		log.Fatalf("Unable to create Kafka Client: %v", err)
	}
	return clusterClient
}

func generateClientConfigs(securityContext string) *sarama.Config {
	clientConfigs := sarama.NewConfig()
	clientConfigs.ClientID = "kafka-idle-topics"
	clientConfigs.Producer.Return.Successes = true
	clientConfigs.Consumer.Return.Errors = true
	clientConfigs.Consumer.Offsets.AutoCommit.Enable = true
	clientConfigs.Consumer.Offsets.AutoCommit.Interval = time.Duration(10) * time.Millisecond
	if securityContext == "plain_tls" {
		clientConfigs.Net.SASL.Enable = true
		clientConfigs.Net.SASL.User = kafkaUsername
		clientConfigs.Net.SASL.Password = kafkaPassword
		clientConfigs.Net.TLS.Enable = true
	} else if securityContext == "plain" {
		clientConfigs.Net.SASL.Enable = true
		clientConfigs.Net.SASL.User = kafkaUsername
		clientConfigs.Net.SASL.Password = kafkaPassword
	} else if securityContext == "tls" {
		clientConfigs.Net.TLS.Enable = true
	}
	return clientConfigs
}

func writeTopicsLocally(topics map[string][]int32) string {
	currentDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("Could not write results due to: %v", err)
	}

	file, err := os.Create(fmt.Sprintf("%s/%s", currentDir, fileName))
	if err != nil {
		log.Fatalf("Could not write results due to: %v", err)
	}
	defer file.Close()

	for topic := range topics {
		_, err := file.WriteString(topic)
		if err != nil {
			log.Printf("WARN: Could not write this topic to file: %s", topic)
		}
		_, err = file.WriteString("\n")
		if err != nil {
			log.Printf("WARN: Could not write this topic to file: %s", topic)
		}
	}
	file.Sync()
	return file.Name()
}

func GetOSEnvVar(env_var string) string {
	key, present := os.LookupEnv(env_var)
	if present && key != "" {
		return key
	}

	panic(errors.New("Environment variable has not been specified: " + env_var))
}

func makeRange(min int32, max int32) []int32 {
	a := make([]int32, max-min+1)
	for i := range a {
		a[i] = min + int32(i)
	}
	return a
}
