Competing consumer library built on top of sarama-cluster

## Installation

`go get -u github.com/uber-go/kafka-client`

## Quick Start

```go
package main

import (
	"os"
	"os/signal"

	"github.com/uber-go/kafka-client"
	"github.com/uber-go/kafka-client/kafka"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

func main() {
	brokers := map[string][]string{
		"sample_cluster":     []string{"127.0.0.1:9092"},
		"sample_dlq_cluster": []string{"127.0.0.1:9092"},
	}
	topicClusterAssignment := map[string][]string{
	    "sample_topic": []string{"sample_cluster"},
	}
	client := kafkaclient.New(kafka.NewStaticNameResolver(brokers, topicClusterAssignment), zap.NewNop(), tally.NoopScope)
	config := &kafka.ConsumerConfig{
		Topic:       "sample_topic",
		Cluster:     "sample_cluster",
		GroupName:   "sample_consumer",
		Concurrency: 100, // number of go routines processing messages in parallel
		DLQ: kafka.DLQConfig{
			Name:    "sample_consumer_dlq",
			Cluster: "sample_dlq_cluster",
		},
	}

	consumer, err := client.NewConsumer(config)
	if err != nil {
		panic(err)
	}

	if err := consumer.Start(); err != nil {
		panic(err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	for {
		select {
		case msg, ok := <-consumer.Messages():
			if !ok {
				return // channel closed
			}
			if err := process(msg); err != nil {
				msg.Nack()
			} else {
				msg.Ack()
			}
		case <-sigCh:
			consumer.Stop()
			<-consumer.Closed()
		}
	}
}
```

