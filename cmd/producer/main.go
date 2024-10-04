package main

import (
	"fmt"
	"log"
	"time"

	"gokafka/env"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type OrderPlacer struct {
	producer           *kafka.Producer
	topic              string
	produceIntervalSec int
	deliveryCh         chan kafka.Event
}

func NewOrderPlacer(p *kafka.Producer, topic string, interval int) *OrderPlacer {
	return &OrderPlacer{
		producer:           p,
		topic:              topic,
		produceIntervalSec: interval,
		deliveryCh:         make(chan kafka.Event, 10000),
	}
}

func (op *OrderPlacer) place(order string, size int) error {
	var (
		format  = fmt.Sprintf("%s - %d", order, size)
		payload = []byte(format)
	)

	err := op.producer.Produce(
		&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &op.topic,
				Partition: kafka.PartitionAny,
			},
			Value: payload,
		},
		op.deliveryCh,
	)
	if err != nil {
		log.Fatalln("failed to produce message: ", err)
	}
	<-op.deliveryCh
	log.Printf("message=%s was delivered", payload)
	time.Sleep(time.Second * time.Duration(op.produceIntervalSec))

	return nil
}

func main() {
	producer, err := kafka.NewProducer(
		&kafka.ConfigMap{
			"bootstrap.servers": env.BOOSTRAP_SERVER,
			"client.id":         "socketname",
			"acks":              "all",
		},
	)
	if err != nil {
		log.Fatalf("Failed to create producer: %s\n", err)
	}

	op := NewOrderPlacer(producer, env.TOPIC, 1)
	for i := 0; i < 1000; i++ {
		if err := op.place("Adidas T-shirt", i+1); err != nil {
			log.Fatalf("order placing failed: %s", err.Error())
		}
	}
}
