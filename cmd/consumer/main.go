package main

import (
	"fmt"
	"log"
	"os"

	"gokafka/env"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	consumer, err := kafka.NewConsumer(
		&kafka.ConfigMap{
			"bootstrap.servers": env.BOOSTRAP_SERVER,
			"group.id":          "foo",
			"auto.offset.reset": "smallest",
		},
	)
	if err != nil {
		log.Fatalf("Faild to create consumer: %s\n", err)
	}
	defer consumer.Close()

	err = consumer.Subscribe(env.TOPIC, nil)
	if err != nil {
		log.Fatalf("Failed to Subscribe on topic=%s: %s", env.TOPIC, err)
	}

	polling := true
	for polling {
		event := consumer.Poll(100)
		switch e := event.(type) {
		case *kafka.Message:
			fmt.Printf("consume message=%s\n", string(e.Value))
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			polling = false
		}
	}
}
