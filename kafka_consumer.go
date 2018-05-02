package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	var kafka_servers *string = flag.String("bootstrap-servers", "some-server.cloud.com:9092", "list of kafka servers")

	var consumer_gid *string = flag.String("groupid", "testing-consumer", "consumer group ID. Default testing-consumer")
	var topic *string = flag.String("topic", "topic1,topic2", "Topics to read, separate by comma")

	var offset_reset *string = flag.String("offset-reset", "latest", "Offset reset -> either latest or first. Check kafka consumer confs. Default is latest")

	flag.Parse()

	//split the topic into a [] slice of strings.

	topic_list := strings.Split(*topic, ",")

	c, err := kafka.NewConsumer(&kafka.ConfigMap{"bootstrap.servers": *kafka_servers, "group.id": *consumer_gid, "auto.offset.reset": *offset_reset})

	if err != nil {
		panic(err)
	}

	c.SubscribeTopics(topic_list, nil)

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			//	matched, err := regexp.MatchString("31", string(*(msg.TopicPartition.Topic)))

			//	if err == nil && matched == true {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
			//	}

		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			break
		}
	}

	c.Close()
}
