// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package kafkaclient

import (
	"fmt"
	"os"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/uber-go/kafka-client/internal/consumer"
	"github.com/uber-go/kafka-client/kafka"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

// Client refers to the kafka client. Serves as
// the entry point to producing or consuming
// messages from kafka
type (
	Client struct {
		tally    tally.Scope
		logger   *zap.Logger
		resolver kafka.NameResolver

		saramaSyncProducerConstructor func([]string) (sarama.SyncProducer, error)
		saramaConsumerConstructor     func([]string, string, []string, *cluster.Config) (consumer.SaramaConsumer, error)
	}
)

// New returns a new kafka client
func New(resolver kafka.NameResolver, logger *zap.Logger, scope tally.Scope) *Client {
	return &Client{
		resolver: resolver,
		logger:   logger,
		tally:    scope,
		saramaSyncProducerConstructor: newSyncProducer,
		saramaConsumerConstructor:     consumer.NewSaramaConsumer,
	}
}

// NewConsumer returns a new instance of kafka consumer
func (c *Client) NewConsumer(config *kafka.ConsumerConfig, options ...ConsumerOption) (kafka.Consumer, error) {
	var err error
	topicList := config.TopicList
	if err = validateTopicListFromSingleCluster(topicList); err != nil {
		return nil, err
	}
	clusterName := topicList[0].Cluster

	opts := buildOptions(config)
	saramaConfig := buildSaramaConfig(&opts)
	msgCh := make(chan kafka.Message, opts.RcvBufferSize)

	topicList, err = c.resolveBrokers(topicList)
	if err != nil {
		return nil, err
	}

	saramaConsumer, err := c.buildSaramaConsumer(topicList, config.GroupName, saramaConfig)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			saramaConsumer.Close()
		}
	}()

	saramaProducerMap, err := c.buildSaramaProducerMap(topicList)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			for _, p := range saramaProducerMap {
				p.Close()
			}
		}
	}()

	dlqMap, err := c.buildDLQMap(topicList, saramaProducerMap)
	if err != nil {
		return nil, err
	}

	return consumer.NewClusterConsumer(
		config.GroupName,
		clusterName,
		&opts,
		topicList,
		msgCh,
		saramaConsumer,
		dlqMap,
		c.tally,
		c.logger,
	)
}

func (c *Client) buildSaramaConsumer(topicList kafka.ConsumerTopicList, consumergroup string, cfg *cluster.Config) (consumer.SaramaConsumer, error) {
	if err := validateTopicListFromSingleCluster(topicList); err != nil {
		return nil, err
	}
	brokerList := topicList[0].BrokerList
	return c.saramaConsumerConstructor(brokerList, consumergroup, topicList.TopicNames(), cfg)
}

func (c *Client) buildSaramaProducerMap(topicList kafka.ConsumerTopicList) (map[string]sarama.SyncProducer, error) {
	var err error
	saramaProducerMap := make(map[string]sarama.SyncProducer)
	for _, consumerTopic := range topicList {
		if consumerTopic.DLQ.Name != "" && consumerTopic.DLQ.Cluster != "" && len(consumerTopic.DLQ.BrokerList) > 0 {
			sp, ok := saramaProducerMap[consumerTopic.DLQ.Cluster]
			if !ok {
				sp, err = c.saramaSyncProducerConstructor(consumerTopic.DLQ.BrokerList)
				if err != nil {
					return nil, fmt.Errorf("failed to create sarama producer for %s", consumerTopic.DLQ.Cluster)
				}
			}
			saramaProducerMap[consumerTopic.DLQ.Cluster] = sp
		}
	}
	return saramaProducerMap, nil
}

func (c *Client) buildDLQMap(topicList kafka.ConsumerTopicList, saramaProducerMap map[string]sarama.SyncProducer) (map[string]consumer.DLQ, error) {
	dlqMap := make(map[string]consumer.DLQ)
	for _, topic := range topicList {
		sp, ok := saramaProducerMap[topic.DLQ.Cluster]
		if !ok {
			return nil, fmt.Errorf("failed to find sarama producer for dlq producer")
		}
		dlq := consumer.NewDLQ(topic.DLQ.Name, topic.DLQ.Cluster, sp, c.tally, c.logger)
		dlqMap[topic.DLQ.HashKey()] = dlq
	}
	return dlqMap, nil
}

// resolveBrokers will attempt to resolve BrokerList for each topic in the inputTopicList.
// If the broker list cannot be resolved, the topic will be removed from the outputTopicList so that
// the consumer will not consume that topic.
func (c *Client) resolveBrokers(inputTopicList kafka.ConsumerTopicList) (kafka.ConsumerTopicList, error) {
	outputTopicList := make([]kafka.ConsumerTopic, 0, len(inputTopicList))
	for _, topic := range inputTopicList {
		if len(topic.BrokerList) == 0 {
			brokers, err := c.resolver.ResolveIPForCluster(topic.Cluster)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve broker IP: %s", err)
			}
			topic.BrokerList = brokers
		}

		if topic.DLQ.Name != "" && len(topic.DLQ.BrokerList) == 0 {
			brokers, err := c.resolver.ResolveIPForCluster(topic.DLQ.Cluster)
			if err != nil {
				return nil, fmt.Errorf("failed to resolve broker IP: %s", err)
			}
			topic.DLQ.BrokerList = brokers
		}

		outputTopicList = append(outputTopicList, topic)
	}
	return outputTopicList, nil
}

// newSyncProducer returns a new sarama sync producer from the cluster config
func newSyncProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Flush.Frequency = time.Millisecond * 500
	return consumer.NewSaramaProducer(brokers, config)
}

func buildOptions(config *kafka.ConsumerConfig, options ...ConsumerOption) consumer.Options {
	opts := defaultOptions
	if config.Concurrency > 0 {
		opts.Concurrency = config.Concurrency
		opts.RcvBufferSize = 2 * opts.Concurrency
	}
	offsetPolicy := config.Offsets.Initial.Offset
	if offsetPolicy == sarama.OffsetNewest || offsetPolicy == sarama.OffsetOldest {
		opts.OffsetPolicy = config.Offsets.Initial.Offset
	}

	for _, option := range options {
		option.apply(&opts)
	}
	return opts
}

func buildSaramaConfig(options *consumer.Options) *cluster.Config {
	config := cluster.NewConfig()
	config.ClientID = clientID()
	config.ChannelBufferSize = options.PartitionRcvBufferSize
	config.Group.Mode = options.ConsumerMode
	config.Group.Return.Notifications = true
	config.Group.Offsets.Synchronization.DwellTime = options.RebalanceDwellTime
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.CommitInterval = options.OffsetCommitInterval
	config.Consumer.Offsets.Initial = options.OffsetPolicy
	config.Consumer.MaxProcessingTime = options.MaxProcessingTime
	return config
}

func clientID() string {
	name, err := os.Hostname()
	if err != nil {
		name = "unknown-kafka-client"
	}
	return name
}

func validateTopicListFromSingleCluster(c kafka.ConsumerTopicList) error {
	if len(c) == 0 {
		return fmt.Errorf("empty topic list")
	}
	cluster := c[0].Cluster
	for _, topic := range c {
		if topic.Cluster != cluster {
			return fmt.Errorf("found two different clusters %s and %s in config", topic.Cluster, cluster)
		}
	}
	return nil
}
