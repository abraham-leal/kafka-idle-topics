package main

import (
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"reflect"
	"strings"
	"time"
)

var KafkaUrl string
var KafkaUsername string
var KafkaPassword string

func GetFlags() {
	flag.StringVar(&KafkaUrl, "bootstrap-servers", "localhost:9092", "Address to the target Kafka Cluster")
	flag.StringVar(&KafkaUsername, "username", "", "Username in the PLAIN module")
	flag.StringVar(&KafkaPassword, "password", "", "Password in the PLAIN module")
}

func main() {
	GetFlags()
	flag.Parse()

	adminClient := getAdminClient("plain_tls")
	defer adminClient.Close()

	noCGnoStorageTopics := noSubscribedGroupsTopics(getNoProductionTopics(getNoStorageTopics(getTopics(adminClient))))

	fmt.Println()
	fmt.Println("Idle topics: ")

	for t,_ := range noCGnoStorageTopics {
		fmt.Println(t)
	}

}

func getNoProductionTopics (topicMetadata map[string]sarama.TopicDetail) map[string]sarama.TopicDetail {
	log.Println("Loading Topics without any active production...")

	beginTopicInspection := map[string]map[int64]int64{}

	clusterClient := getClient("plain_tls")
	for t, td := range topicMetadata {
		thisTopicsPartitions := makeRange(0,td.NumPartitions-1)
		thisTopicCounts := map[int64]int64{}
		for _, partition := range thisTopicsPartitions {
			newestOffsetForPartition, err := clusterClient.GetOffset(t, partition, sarama.OffsetNewest)
			if err != nil {
				log.Fatalf("Could not determine topic end offset: %v", err)
			}
			thisTopicCounts[int64(partition)] = newestOffsetForPartition
		}
		beginTopicInspection[t] = thisTopicCounts
	}

	time.Sleep(time.Duration(500) * time.Millisecond)

	endTopicInspection := map[string]map[int64]int64{}

	for t, td := range topicMetadata {
		thisTopicsPartitions := makeRange(0,td.NumPartitions-1)
		thisTopicCounts := map[int64]int64{}
		for _, partition := range thisTopicsPartitions {
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
		}
	}

	return topicMetadata
}

func getNoStorageTopics(topicMetadata map[string]sarama.TopicDetail) map[string]sarama.TopicDetail {
	log.Println("Loading Topics without anything in them...")

	clusterClient := getClient("plain_tls")
	for t, td := range topicMetadata {
		thisTopicsPartitions := makeRange(0,td.NumPartitions-1)
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
				delete(topicMetadata, t)
			}
		}
	}

	return topicMetadata
}

func getTopics (adminClient sarama.ClusterAdmin) map[string]sarama.TopicDetail {
	log.Println("Loading Topics...")
	topicMetadata, err := adminClient.ListTopics()
	if err != nil {
		log.Fatalf("Could not reach cluster within the last 30 seconds. Is the configuration correct? %v", err)
	}
	return topicMetadata
}

func noSubscribedGroupsTopics(topics map[string]sarama.TopicDetail) map[string]sarama.TopicDetail {
	log.Println("Loading Topics without active Consumer Groups...")
	adminClient := getAdminClient("plain_tls")
	allTopicsToReturn := topics
	allTopicsMap := map[string][]int32{}
	for t, td := range topics {
		allTopicsMap[t] = makeRange(0, td.NumPartitions-1)
	}
	allConsumerGroups, err := adminClient.ListConsumerGroups()
	if err != nil {
		log.Fatalf("Could not obtain Consumer Groups from cluster: %v", err)
	}
	for cg := range allConsumerGroups {
		result, err := adminClient.ListConsumerGroupOffsets(cg, allTopicsMap)
		if err != nil {
			log.Fatalf("Cannot determine if topic is in use by consumers.")
		}

		for topic, partitionData := range result.Blocks {
			seen := false
			for _, dataset := range partitionData {
				if dataset.Offset != -1 {
					delete(allTopicsToReturn, topic)
					seen = true
					continue
				}
				if seen == true {
					continue
				}
			}
			if seen == true {
				continue
			}
		}
	}

	return allTopicsToReturn
}

func getAdminClient(securityContext string) sarama.ClusterAdmin {
	clientConfigs := sarama.NewConfig()
	clientConfigs.ClientID = "idleTopicChecker"
	if securityContext == "plain_tls" {
		clientConfigs.Net.SASL.Enable = true
		clientConfigs.Net.SASL.User = KafkaUsername
		clientConfigs.Net.SASL.Password = KafkaPassword
		clientConfigs.Net.TLS.Enable = true
	} else if securityContext == "plain" {
		clientConfigs.Net.SASL.Enable = true
		clientConfigs.Net.SASL.User = KafkaUsername
		clientConfigs.Net.SASL.Password = KafkaPassword
	} else if securityContext == "tls" {
		clientConfigs.Net.TLS.Enable = true
	}

	brokerUrls := strings.Split(KafkaUrl,",")
	adminClient, err := sarama.NewClusterAdmin(brokerUrls, clientConfigs)
	if err != nil {
		log.Fatalf("Unable to create Kafka Client: %v", err)
	}
	return adminClient
}

func getClient(securityContext string) sarama.Client {
	clientConfigs := sarama.NewConfig()
	clientConfigs.ClientID = "idleTopicChecker"
	if securityContext == "plain_tls" {
		clientConfigs.Net.SASL.Enable = true
		clientConfigs.Net.SASL.User = KafkaUsername
		clientConfigs.Net.SASL.Password = KafkaPassword
		clientConfigs.Net.TLS.Enable = true
	} else if securityContext == "plain" {
		clientConfigs.Net.SASL.Enable = true
		clientConfigs.Net.SASL.User = KafkaUsername
		clientConfigs.Net.SASL.Password = KafkaPassword
	} else if securityContext == "tls" {
		clientConfigs.Net.TLS.Enable = true
	}

	brokerUrls := strings.Split(KafkaUrl,",")
	client, err := sarama.NewClient(brokerUrls, clientConfigs)
	if err != nil {
		log.Fatalf("Unable to create Kafka Client: %v", err)
	}
	return client
}

func makeRange(min int32 , max int32) []int32 {
	a := make([]int32, max-min+1)
	for i := range a {
		a[i] = min + int32(i)
	}
	return a
}
