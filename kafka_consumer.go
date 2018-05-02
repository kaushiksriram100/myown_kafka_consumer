package main

import (
	"flag"
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	var kafka_servers *string = flag.String("bootstrap-servers", "kafka-197807230-1-321969444.prod-cdc.kafka-tf-medusa-prod.ms-df-messaging.cdcprod7.prod.walmart.com:9092,kafka-197807218-1-321969034.prod-cdc.kafka-tf-medusa-prod.ms-df-messaging.cdcprod5.prod.walmart.com:9092,kafka-197807230-3-321969450.prod-cdc.kafka-tf-medusa-prod.ms-df-messaging.cdcprod7.prod.walmart.com:9092,kafka-197807230-2-321969447.prod-cdc.kafka-tf-medusa-prod.ms-df-messaging.cdcprod7.prod.walmart.com:9092,kafka-197807224-5-322168148.prod-cdc.kafka-tf-medusa-prod.ms-df-messaging.cdcprod6.prod.walmart.com:9092,kafka-197807218-5-322167974.prod-cdc.kafka-tf-medusa-prod.ms-df-messaging.cdcprod5.prod.walmart.com:9092,kafka-197807230-4-322168319.prod-cdc.kafka-tf-medusa-prod.ms-df-messaging.cdcprod7.prod.walmart.com:9092,kafka-197807230-5-322168322.prod-cdc.kafka-tf-medusa-prod.ms-df-messaging.cdcprod7.prod.walmart.com:9092,kafka-197807218-3-321969040.prod-cdc.kafka-tf-medusa-prod.ms-df-messaging.cdcprod5.prod.walmart.com:9092,kafka-197807218-2-321969037.prod-cdc.kafka-tf-medusa-prod.ms-df-messaging.cdcprod5.prod.walmart.com:9092,kafka-197807224-3-321969245.prod-cdc.kafka-tf-medusa-prod.ms-df-messaging.cdcprod6.prod.walmart.com:9092,kafka-197807224-4-322168145.prod-cdc.kafka-tf-medusa-prod.ms-df-messaging.cdcprod6.prod.walmart.com:9092,kafka-197807224-1-321969239.prod-cdc.kafka-tf-medusa-prod.ms-df-messaging.cdcprod6.prod.walmart.com:9092,kafka-197807224-2-321969242.prod-cdc.kafka-tf-medusa-prod.ms-df-messaging.cdcprod6.prod.walmart.com:9092,kafka-197807218-4-322167971.prod-cdc.kafka-tf-medusa-prod.ms-df-messaging.cdcprod5.prod.walmart.com:9092", "list of kafka servers")

	var consumer_gid *string = flag.String("groupid", "testing-consumer", "consumer group ID. Default testing-consumer")
	var topic *string = flag.String("topic", "tf.medusa.metric.1,tf.medusa.metric.2", "Topics to read, separate by comma")

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
