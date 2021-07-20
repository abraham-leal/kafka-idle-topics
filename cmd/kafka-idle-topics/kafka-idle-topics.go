package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type KafkaIdleTopics struct {
	 kafkaUrl string
	 kafkaUsername string
	 kafkaPassword string
	 kafkaSecurity string
	 fileName string
	 skip string
	 productionAssessmentTime int
	 hideInternalTopics bool
	 topicsIdleMinutes int64
	 waitForTopicEvaluation sync.WaitGroup
	 topicPartitionMap map[string][]int32
	 DeleteCandidates map[string]bool
}

func NewKafkaIdleTopics () *KafkaIdleTopics {
	thisInstance := KafkaIdleTopics{}
	thisInstance.DeleteCandidates = make(map[string]bool)
	return &thisInstance
}

func ReadCommands() *KafkaIdleTopics {
	thisInstance := NewKafkaIdleTopics()

	flag.StringVar(&thisInstance.kafkaUrl, "bootstrap-servers", "", "Address to the target Kafka Cluster. Accepts multiple endpoints separated by a comma.")
	flag.StringVar(&thisInstance.kafkaUsername, "username", "", "Username in the PLAIN module.")
	flag.StringVar(&thisInstance.kafkaPassword, "password", "", "Password in the PLAIN module.")
	flag.StringVar(&thisInstance.kafkaSecurity, "kafkaSecurity", "none", "Type of connection to attempt. Options: plain_tls, plain (no tls), tls (one-way), none.")
	flag.StringVar(&thisInstance.fileName, "filename", "idleTopics.txt", "Custom filename for the output if needed.")
	flag.StringVar(&thisInstance.skip, "skip", "", "Filtering to skip. Options are: production, consumption, storage. This can be a comma-delimited list.")
	flag.IntVar(&thisInstance.productionAssessmentTime, "productionAssessmentTimeMs", 30000, "Timeframe to assess active production.")
	flag.Int64Var(&thisInstance.topicsIdleMinutes, "idleMinutes", 0, "Amount of minutes a topic should be idle to report it.")
	flag.BoolVar(&thisInstance.hideInternalTopics, "hideInternalTopics", false, "Hide internal topics from assessment.")

	flag.Parse()

	return thisInstance
}

func main() {

	myChecker := ReadCommands()

	if myChecker.kafkaSecurity == "plain_tls" || myChecker.kafkaSecurity == "plain" {
		// If the parameters are empty, go fetch from env
		if myChecker.kafkaUrl == "" || myChecker.kafkaUsername == "" || myChecker.kafkaPassword == "" {
			myChecker.kafkaUrl = GetOSEnvVar("KAFKA_BOOTSTRAP")
			myChecker.kafkaUsername = GetOSEnvVar("KAFKA_USERNAME")
			myChecker.kafkaPassword = GetOSEnvVar("KAFKA_PASSWORD")
		}
	}

	adminClient := myChecker.getAdminClient(myChecker.kafkaSecurity)
	clusterClient := myChecker.getClusterClient(myChecker.kafkaSecurity)
	defer adminClient.Close()
	defer clusterClient.Close()

	stepsToSkip := strings.Split(myChecker.skip, ",")

	// Extract Topics in Cluster
	myChecker.topicPartitionMap = myChecker.getClusterTopics(adminClient)

	if myChecker.hideInternalTopics {
		for t := range myChecker.topicPartitionMap {
			if strings.HasPrefix(t, "_") {
				delete(myChecker.topicPartitionMap, t)
			}
		}
	}

	if !isInSlice("production", stepsToSkip) {
		if myChecker.topicsIdleMinutes == 0 {
			myChecker.filterActiveProductionTopics(clusterClient)
		} else {
			myChecker.filterTopicsIdleSince(clusterClient)
		}
	}

	if !isInSlice("consumption", stepsToSkip) {
		myChecker.filterTopicsWithConsumerGroups(adminClient)
	}

	if !isInSlice("storage", stepsToSkip) {
		myChecker.filterEmptyTopics(clusterClient)
	}

	myChecker.filterOutDeleteCandidates()

	path := myChecker.writeDeleteCandidatesLocally()

	partitionCount := 0
	for _, ps := range myChecker.topicPartitionMap {
		partitionCount = partitionCount + len(ps)
	}

	log.Printf("Done! You can delete %v topics and %v partitions! A list of found idle topics is available at: %s", len(myChecker.topicPartitionMap), partitionCount, path)
}

/*
	Uses the provided Sarama Admin Client to get a list of current topics in the cluster
*/
func (c *KafkaIdleTopics) getClusterTopics(adminClient sarama.ClusterAdmin) map[string][]int32 {
	log.Println("Loading Topics...")
	topicMetadata, err := adminClient.ListTopics()
	if err != nil {
		log.Fatalf("Could not reach cluster within the last 30 seconds. Is the configuration correct? %v", err)
	}

	c.topicPartitionMap = map[string][]int32{}
	for t, td := range topicMetadata {
		c.topicPartitionMap[t] = makeRange(0, td.NumPartitions-1)
	}

	return c.topicPartitionMap
}

/*
	Adds topics with nothing stored in them to c.DeleteCandidates
	It is also possible for this method to remove candidacy if it detects activity.
*/
func (c *KafkaIdleTopics) filterEmptyTopics(clusterClient sarama.Client) {
	log.Println("Evaluating Topics without anything in them...")

	for topic, td := range c.topicPartitionMap {
		thisTopicsPartitions := td
		for _, partition := range thisTopicsPartitions {
			oldestOffsetForPartition, err := clusterClient.GetOffset(topic, partition, sarama.OffsetOldest)
			if err != nil {
				log.Fatalf("Could not determine topic storage: %v", err)
			}
			newestOffsetForPartition, err := clusterClient.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				log.Fatalf("Could not determine topic storage: %v", err)
			}
			if oldestOffsetForPartition != newestOffsetForPartition {
				c.DeleteCandidates[topic] = false
				break
			} else {
				v, e := c.DeleteCandidates[topic]
				if !e  {
					c.DeleteCandidates[topic] = true
				} else if e && v != false {
					c.DeleteCandidates[topic] = true
				}
			}
		}
	}
}

/*
	Adds topics that aren't being actively produced to c.DeleteCandidates
	It is also possible for this method to remove candidacy if it detects activity.
*/
func (c *KafkaIdleTopics) filterActiveProductionTopics(clusterClient sarama.Client) {
	log.Println("Evaluating Topics without any active production...")

	beginTopicInspection := map[string]map[int64]int64{}

	for t := range c.topicPartitionMap {
		thisTopicCounts := map[int64]int64{}
		for _, partition := range c.topicPartitionMap[t] {
			newestOffsetForPartition, err := clusterClient.GetOffset(t, partition, sarama.OffsetNewest)
			if err != nil {
				log.Fatalf("Could not determine topic end offset: %v", err)
			}
			thisTopicCounts[int64(partition)] = newestOffsetForPartition
		}
		beginTopicInspection[t] = thisTopicCounts
	}

	// Sleep for configurable time to see if the offsets grow
	log.Printf("Waiting for %v ms to evaluate active production.", c.productionAssessmentTime)
	time.Sleep(time.Duration(c.productionAssessmentTime) * time.Millisecond)

	endTopicInspection := map[string]map[int64]int64{}

	for t := range c.topicPartitionMap {
		thisTopicCounts := map[int64]int64{}
		for _, partition := range c.topicPartitionMap[t] {
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
			c.DeleteCandidates[topic] = false
		} else {
			v, e := c.DeleteCandidates[topic]
			if !e {
				c.DeleteCandidates[topic] = true
			} else if e && v != false {
				c.DeleteCandidates[topic] = true
			}
		}
	}
}

/*
	Adds topics that do not have any consumer groups to c.DeleteCandidates
	It is also possible for this method to remove candidacy if it detects activity.
*/
func (c *KafkaIdleTopics) filterTopicsWithConsumerGroups(adminClient sarama.ClusterAdmin) {
	log.Println("Evaluating Topics without active Consumer Groups...")
	allConsumerGroups, err := adminClient.ListConsumerGroups()
	if err != nil {
		log.Fatalf("Could not obtain Consumer Groups from cluster: %v", err)
	}

	for cg := range allConsumerGroups {
		result, err := adminClient.ListConsumerGroupOffsets(cg, c.topicPartitionMap)
		if err != nil {
			log.Fatalf("Cannot determine if topic is in use by consumers.")
		}

		for topic, partitionData := range result.Blocks {
			for _, dataset := range partitionData {
				if dataset.Offset != -1 {
					c.DeleteCandidates[topic] = false
					break
				} else {
					v, e := c.DeleteCandidates[topic]
					if !e {
						c.DeleteCandidates[topic] = true
					} else if e && v != false {
						c.DeleteCandidates[topic] = true
					}
				}
			}
		}
	}
}

/*
	Adds topics that have not been produced to since a c.timeMinutesSince to c.DeleteCandidates
	It is also possible for this method to remove candidacy if it detects activity.
 */
func (c *KafkaIdleTopics) filterTopicsIdleSince (clusterClient sarama.Client) {
	log.Printf("Evaluating Topics that haven't been produced to since... %v", time.Now().Add(-time.Duration(c.topicsIdleMinutes) * time.Minute))

	evaluatingConsumer, err := sarama.NewConsumerFromClient(clusterClient)
	if err != nil {
		log.Fatalln("Could not consume from cluster to evaluate")
	}
	defer evaluatingConsumer.Close()

	for topic, td := range c.topicPartitionMap {
		thisTopicsPartitions := td
		thisTopicConsumers := []sarama.PartitionConsumer{}

		for _, partition := range thisTopicsPartitions {
			pcons, err := evaluatingConsumer.ConsumePartition(topic, partition, sarama.OffsetNewest-1)
			if err != nil {
				log.Fatalf("Could not consume from topic: %v", err)
			}
			thisTopicConsumers = append(thisTopicConsumers, pcons)
		}
		c.waitForTopicEvaluation.Add(1)
		go c.evaluateTopicTimes(thisTopicConsumers, topic, &c.waitForTopicEvaluation)
	}

	c.waitForTopicEvaluation.Wait()
}

/*
	Helper method to filterTopicsIdleSince
 */
func (c *KafkaIdleTopics) evaluateTopicTimes(pcons []sarama.PartitionConsumer, topic string, group *sync.WaitGroup) {
	defer group.Done()
	partitionHasSomething := []bool{}

	for _, pcon := range pcons {
		select {
			case msg := <- pcon.Messages():
				if msg.Timestamp.Before(time.Now().Add(-time.Duration(c.topicsIdleMinutes) * time.Minute)) { // If last produced message has timestamp before deadline
					partitionHasSomething = append(partitionHasSomething, false) // Add topic as candidate
				} else {
					partitionHasSomething = append(partitionHasSomething, true) // Else: Remove
				}
				break
			case <-time.After(time.Duration(5) * time.Second):
				partitionHasSomething = append(partitionHasSomething, false) // If times out, add as candidate
		}
		pcon.AsyncClose()
	}

	for _, value := range partitionHasSomething {
		if value == true {
			c.DeleteCandidates[topic] = false
			return
		}
	}

	v, e := c.DeleteCandidates[topic]
	if !e {
		c.DeleteCandidates[topic] = true
	} else if e && v != false {
		c.DeleteCandidates[topic] = true
	}
}

/*
	Filters c.topicPartitionMap to include the same topics as c.DeleteCandidates
	and cleans c.DeleteCandidates to only include topics to be removed.
 */
func (c *KafkaIdleTopics) filterOutDeleteCandidates (){

	for t := range c.topicPartitionMap {
		v, existsInCandidates := c.DeleteCandidates[t]
		if existsInCandidates && !v {
			delete(c.topicPartitionMap, t)
			delete(c.DeleteCandidates, t)
		} else if !existsInCandidates {
			delete(c.topicPartitionMap, t)
		}
	}
}

func (c *KafkaIdleTopics) getAdminClient(securityContext string) sarama.ClusterAdmin {
	adminClient, err := sarama.NewClusterAdmin(strings.Split(c.kafkaUrl, ","), c.generateClientConfigs(securityContext))
	if err != nil {
		log.Fatalf("Unable to create Kafka Client: %v", err)
	}
	return adminClient
}

func (c *KafkaIdleTopics) getClusterClient(securityContext string) sarama.Client {
	clusterClient, err := sarama.NewClient(strings.Split(c.kafkaUrl, ","), c.generateClientConfigs(securityContext))
	if err != nil {
		log.Fatalf("Unable to create Kafka Client: %v", err)
	}
	return clusterClient
}

func (c *KafkaIdleTopics) generateClientConfigs(securityContext string) *sarama.Config {
	clientConfigs := sarama.NewConfig()
	clientConfigs.ClientID = "kafka-idle-topics"
	clientConfigs.Producer.Return.Successes = true
	clientConfigs.Consumer.Return.Errors = true
	clientConfigs.Consumer.Offsets.AutoCommit.Enable = true
	clientConfigs.Consumer.Offsets.AutoCommit.Interval = time.Duration(10) * time.Millisecond
	if securityContext == "plain_tls" {
		clientConfigs.Net.SASL.Enable = true
		clientConfigs.Net.SASL.User = c.kafkaUsername
		clientConfigs.Net.SASL.Password = c.kafkaPassword
		clientConfigs.Net.TLS.Enable = true
	} else if securityContext == "plain" {
		clientConfigs.Net.SASL.Enable = true
		clientConfigs.Net.SASL.User = c.kafkaUsername
		clientConfigs.Net.SASL.Password = c.kafkaPassword
	} else if securityContext == "tls" {
		clientConfigs.Net.TLS.Enable = true
	}
	return clientConfigs
}

func (c *KafkaIdleTopics) writeDeleteCandidatesLocally () string {
	currentDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("Could not write results due to: %v", err)
	}

	file, err := os.Create(fmt.Sprintf("%s/%s", currentDir, c.fileName))
	if err != nil {
		log.Fatalf("Could not write results due to: %v", err)
	}
	defer file.Close()

	for topic := range c.DeleteCandidates {
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

func isInSlice(i string, list []string) bool {
	for _, current := range list {
		if current == i {
			return true
		}
	}
	return false
}

func makeRange(min int32, max int32) []int32 {
	a := make([]int32, max-min+1)
	for i := range a {
		a[i] = min + int32(i)
	}
	return a
}
