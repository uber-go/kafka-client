# Go Kafka Client Library [![Mit License][mit-img]][mit] [![Build Status][ci-img]][ci] [![Coverage Status][cov-img]][cov]

A high level Go client library for Apache Kafka that provides the following primitives on top of [sarama-cluster](https://github.com/bsm/sarama-cluster):

* Competing consumer semantics with dead letter queue (DLQ)
  * Ability to process messages across multiple goroutines
  * Ability to Ack or Nack messages out of order (with optional DLQ)
* Ability to consume from topics spread across different kafka clusters

## Stability

This library is in alpha. APIs are subject to change, use at your own risk

## Contributing
If you are interested in contributing, please sign the [License Agreement](https://cla-assistant.io/uber-go/kafka-client) and see our [development guide](https://github.com/uber-go/kafka-client/blob/master/docs/DEVELOPMENT-GUIDE.md)

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
	// mapping from cluster name to list of broker ip addresses
	brokers := map[string][]string{
		"sample_cluster":     []string{"127.0.0.1:9092"},
		"sample_dlq_cluster": []string{"127.0.0.1:9092"},
	}
	// mapping from topic name to cluster that has that topic
	topicClusterAssignment := map[string][]string{
		"sample_topic": []string{"sample_cluster"},
	}

	// First create the kafkaclient, its the entry point for creating consumers or producers
	// It takes as input a name resolver that knows how to map topic names to broker ip addrs
	client := kafkaclient.New(kafka.NewStaticNameResolver(topicClusterAssignment, brokers), zap.NewNop(), tally.NoopScope)

	// Next, setup the consumer config for consuming from a set of topics
	config := &kafka.ConsumerConfig{
		TopicList: kafka.ConsumerTopicList{
			kafka.ConsumerTopic{ // Consumer Topic is a combination of topic + dead-letter-queue
				Topic: kafka.Topic{ // Each topic is a tuple of (name, clusterName)
					Name:    "sample_topic",
					Cluster: "sample_cluster",
				},
				DLQ: kafka.Topic{
					Name:    "sample_consumer_dlq",
					Cluster: "sample_dlq_cluster",
				},
			},
		},
		GroupName:   "sample_consumer",
		Concurrency: 100, // number of go routines processing messages in parallel
	}

	// receive all messages on the topic
	config.Offsets.Initial.Offset = kafka.OffsetOldest

	// Create the consumer through the previously created client
	consumer, err := client.NewConsumer(config)
	if err != nil {
		panic(err)
	}

	// Finally, start consuming
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

[mit-img]: http://img.shields.io/badge/License-MIT-blue.svg
[mit]: https://github.com/uber-go/kafka-client/blob/master/LICENSE

[ci-img]: https://img.shields.io/travis/uber-go/kafka-client/master.svg
[ci]: https://travis-ci.org/uber-go/kafka-client/branches

[cov-img]: https://codecov.io/gh/uber-go/kafka-client/branch/master/graph/badge.svg
[cov]: https://codecov.io/gh/uber-go/kafka-client/branch/master
