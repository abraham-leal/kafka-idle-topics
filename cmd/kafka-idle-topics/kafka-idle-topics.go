package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

func NewKafkaIdleTopics() *KafkaIdleTopics {
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
	versionFlag := flag.Bool("version", false, "Print the current version and exit")
	flag.Var(&AllowList, "allowList", "A comma delimited list of topics to evaluate. It also accepts a path to a file containing a list of topics.")
	flag.Var(&DisallowList, "disallowList", "A comma delimited list of topics to exclude from evaluation. It also accepts a path to a file containing a list of topics.")

	flag.Parse()

	if *versionFlag {
		fmt.Printf("kafka-idle-topics: %s\n", Version)
		os.Exit(0)
	}

	return thisInstance
}

func main() {

	myChecker := ReadCommands()

	if myChecker.kafkaSecurity == "plain_tls" || myChecker.kafkaSecurity == "plain" {
		// If the parameters are empty, go fetch from env
		if myChecker.kafkaUrl == "" || myChecker.kafkaUsername == "" || myChecker.kafkaPassword == "" {
			myChecker.kafkaUsername, _ = GetOSEnvVar("KAFKA_BOOTSTRAP")
			myChecker.kafkaUsername, _ = GetOSEnvVar("KAFKA_USERNAME")
			myChecker.kafkaPassword, _ = GetOSEnvVar("KAFKA_PASSWORD")
		}
	}

	if myChecker.topicsIdleMinutes == 0 {
		envVar, err := GetOSEnvVar("KAFKA_IDLE_MINUTES")
		if err != nil {
			log.Printf("%s, using default of 0\n", err)
			myChecker.topicsIdleMinutes = 0
		} else {
			idleInt, err := strconv.ParseInt(envVar, 10, 64)
			if err != nil {
				log.Printf("Couldn't parse env var %v, using default of 0", err)
				myChecker.topicsIdleMinutes = 0
			}
			myChecker.topicsIdleMinutes = idleInt
		}
	}

	stepsToSkip := strings.Split(myChecker.skip, ",")

	// Extract Topics in Cluster
	myChecker.topicPartitionMap = myChecker.getClusterTopics(myChecker.getAdminClient(myChecker.kafkaSecurity))

	if myChecker.hideInternalTopics {
		for t := range myChecker.topicPartitionMap {
			if strings.HasPrefix(t, "_") {
				delete(myChecker.topicPartitionMap, t)
			}
		}
	}

	if !isInSlice("production", stepsToSkip) {
		if myChecker.topicsIdleMinutes == 0 {
			myChecker.filterActiveProductionTopics(myChecker.getClusterClient(myChecker.kafkaSecurity))
		} else {
			myChecker.filterTopicsIdleSince(myChecker.getClusterClient(myChecker.kafkaSecurity))
		}
	}

	if !isInSlice("consumption", stepsToSkip) {
		myChecker.filterTopicsWithConsumerGroups(myChecker.getAdminClient(myChecker.kafkaSecurity))
	}

	if !isInSlice("storage", stepsToSkip) {
		myChecker.filterEmptyTopics(myChecker.getClusterClient(myChecker.kafkaSecurity))
	}

	myChecker.filterOutDeleteCandidates()

	path := myChecker.writeDeleteCandidatesLocally()

	partitionCount := 0
	for _, ps := range myChecker.topicPartitionMap {
		partitionCount = partitionCount + len(ps)
	}

	log.Printf("Done! You can delete %v topics and %v partitions! A list of found idle topics is available at: %s", len(myChecker.topicPartitionMap), partitionCount, path)
}
