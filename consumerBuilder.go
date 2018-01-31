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
	"errors"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/uber-go/kafka-client/internal/consumer"
	"github.com/uber-go/kafka-client/kafka"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type (
	consumerBuilder struct {
		buildErrors *consumerBuildErrorList
		topics      kafka.ConsumerTopicList

		saramaConsumerMap  map[string]consumer.SaramaConsumer
		saramaProducerMap  map[string]sarama.SyncProducer
		clusterConsumerMap map[string]kafka.Consumer

		msgCh        chan kafka.Message
		logger       *zap.Logger
		scope        tally.Scope
		config       *kafka.ConsumerConfig
		opts         *consumer.Options
		saramaConfig *cluster.Config
	}

	consumerBuildErrorList struct {
		errs []ConsumerBuildError
	}

	// ConsumerBuildError is an error that encapsulates a Topic that could not be consumed
	// due to some error.
	ConsumerBuildError struct {
		Topic kafka.ConsumerTopic
		error
	}
)

// HasConsumerBuildError can be used to test whether NewConsumer returned a consumer
// consuming from a subset of requested topics.
func HasConsumerBuildError(err error) []ConsumerBuildError {
	be := err.(*consumerBuildErrorList)
	return be.errs
}

func newConsumerBuilder(logger *zap.Logger, scope tally.Scope, config *kafka.ConsumerConfig, opts *consumer.Options) *consumerBuilder {
	saramaConfig := buildSaramaConfig(opts)
	return &consumerBuilder{
		buildErrors:        newConsumerBuildErrorList(),
		topics:             config.TopicList,
		saramaConsumerMap:  nil,
		saramaProducerMap:  nil,
		clusterConsumerMap: nil,
		msgCh:              make(chan kafka.Message, opts.RcvBufferSize),
		logger:             logger,
		scope:              scope,
		config:             config,
		opts:               opts,
		saramaConfig:       saramaConfig,
	}
}

func (c *consumerBuilder) build() (kafka.Consumer, error) {
	mc, err := consumer.NewMultiClusterConsumer(
		c.config,
		c.topics,
		c.clusterConsumerMap,
		c.saramaConsumerMap,
		c.saramaProducerMap,
		c.msgCh,
		c.scope,
		c.logger,
	)
	if err != nil {
		return nil, err
	}
	return mc, c.buildErrors.ToError()
}

func (c *consumerBuilder) buildClusterConsumerMap(f func(string, string, *consumer.Options, kafka.ConsumerTopicList, chan kafka.Message, consumer.SaramaConsumer, map[string]consumer.DLQ, tally.Scope, *zap.Logger) (kafka.Consumer, error)) error {
	clusterConsumerMap := make(map[string]kafka.Consumer)
	outputTopicList := make([]kafka.ConsumerTopic, 0, len(c.topics))

	clusterTopicsMap := c.clusterTopicsMap()
	for cluster, topicList := range clusterTopicsMap {
		sc, ok := c.saramaConsumerMap[cluster]
		if !ok {
			return errors.New("clusterConsumer builder failed to find expected sarama consumer")
		}

		dlqMap := make(map[string]consumer.DLQ)
		for _, topic := range topicList {
			sp, ok := c.saramaProducerMap[topic.DLQ.Cluster]
			if !ok {
				return errors.New("clusterConsumer builder failed to find expected sarama producer")
			}
			dlqMap[topic.DLQ.HashKey()] = consumer.NewDLQ(topic.DLQ.Name, topic.DLQ.Cluster, sp, c.scope, c.logger)
		}

		cc, err := f(c.config.GroupName, cluster, c.opts, topicList, c.msgCh, sc, dlqMap, c.scope, c.logger)
		if err != nil {
			c.buildErrors.AddAll(topicList, err)
			continue
		}

		clusterConsumerMap[cluster] = cc
		outputTopicList = append(outputTopicList, topicList...)
	}

	c.clusterConsumerMap = clusterConsumerMap
	c.topics = outputTopicList
	return nil
}

func (c *consumerBuilder) buildSaramaProducerMap(f func([]string) (sarama.SyncProducer, error)) {
	outputTopicList := make([]kafka.ConsumerTopic, 0, 10)
	saramaProducerMap := make(map[string]sarama.SyncProducer)

	dlqClusterTopicsMap := c.dlqClusterTopicsMap()
	for cluster, topicList := range dlqClusterTopicsMap {
		brokerList := topicList[0].DLQ.BrokerList
		sp, err := f(brokerList)
		if err != nil {
			c.buildErrors.AddAll(topicList, err)
			continue
		}
		saramaProducerMap[cluster] = sp
		outputTopicList = append(outputTopicList, topicList...)
	}

	c.topics = outputTopicList
	c.saramaProducerMap = saramaProducerMap
}

func (c *consumerBuilder) buildSaramaConsumerMap(f func([]string, string, []string, *cluster.Config) (consumer.SaramaConsumer, error)) {
	outputTopicList := make([]kafka.ConsumerTopic, 0, 10)
	saramaConsumerMap := make(map[string]consumer.SaramaConsumer)

	clusterTopicMap := c.clusterTopicsMap()
	for cluster, topicList := range clusterTopicMap {
		brokerList := topicList[0].BrokerList
		sc, err := f(brokerList, c.config.GroupName, topicList.TopicNames(), c.saramaConfig)
		if err != nil {
			c.buildErrors.AddAll(topicList, err)
			continue
		}
		saramaConsumerMap[cluster] = sc
		outputTopicList = append(topicList, topicList...)
	}

	c.topics = outputTopicList
	c.saramaConsumerMap = saramaConsumerMap
}

// resolveBrokers will attempt to resolve BrokerList for each topic in the inputTopicList.
// If the broker list cannot be resolved, the topic will be removed from the outputTopicList so that
// the consumer will not consume that topic.
func (c *consumerBuilder) resolveBrokers(resolver kafka.NameResolver) {
	outputTopicList := make([]kafka.ConsumerTopic, 0, len(c.topics))
	for _, topic := range c.topics {
		if len(topic.BrokerList) == 0 {
			brokers, err := resolver.ResolveIPForCluster(topic.Cluster)
			if err != nil {
				c.buildErrors.Add(topic, err)
				continue
			}
			topic.BrokerList = brokers
		}

		if topic.DLQ.Name != "" && len(topic.DLQ.BrokerList) == 0 {
			brokers, err := resolver.ResolveIPForCluster(topic.DLQ.Cluster)
			if err != nil {
				c.buildErrors.Add(topic, err)
				continue
			}
			topic.DLQ.BrokerList = brokers
		}

		outputTopicList = append(outputTopicList, topic)
	}

	c.topics = outputTopicList
}

func (c *consumerBuilder) dlqClusterTopicsMap() map[string]kafka.ConsumerTopicList {
	clusterTopicMap := make(map[string]kafka.ConsumerTopicList)
	for _, consumerTopic := range c.topics {
		topicList, ok := clusterTopicMap[consumerTopic.DLQ.Cluster]
		if !ok {
			topicList = make([]kafka.ConsumerTopic, 0, 10)
		}
		topicList = append(topicList, consumerTopic)
		clusterTopicMap[consumerTopic.DLQ.Cluster] = topicList
	}
	return clusterTopicMap
}

func (c *consumerBuilder) clusterTopicsMap() map[string]kafka.ConsumerTopicList {
	clusterTopicMap := make(map[string]kafka.ConsumerTopicList)
	for _, consumerTopic := range c.topics {
		topicList, ok := clusterTopicMap[consumerTopic.Cluster]
		if !ok {
			topicList = make([]kafka.ConsumerTopic, 0, 10)
		}
		topicList = append(topicList, consumerTopic)
		clusterTopicMap[consumerTopic.Cluster] = topicList
	}
	return clusterTopicMap
}

func buildOptions(config *kafka.ConsumerConfig) *consumer.Options {
	opts := consumer.DefaultOptions()
	if config.Concurrency > 0 {
		opts.Concurrency = config.Concurrency
		opts.RcvBufferSize = 2 * opts.Concurrency
	}
	offsetPolicy := config.Offsets.Initial.Offset
	if offsetPolicy == sarama.OffsetNewest || offsetPolicy == sarama.OffsetOldest {
		opts.OffsetPolicy = config.Offsets.Initial.Offset
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

func newConsumerBuildErrorList() *consumerBuildErrorList {
	return &consumerBuildErrorList{
		errs: make([]ConsumerBuildError, 0, 10),
	}
}

func (c *consumerBuildErrorList) AddAll(topics kafka.ConsumerTopicList, err error) {
	for _, topic := range topics {
		c.errs = append(c.errs, ConsumerBuildError{Topic: topic, error: err})
	}
}

func (c *consumerBuildErrorList) Add(topic kafka.ConsumerTopic, err error) {
	c.errs = append(c.errs, ConsumerBuildError{Topic: topic, error: err})
}

func (c *consumerBuildErrorList) Error() string {
	return "Building NewConsumer has error"
}

func (c *consumerBuildErrorList) ToError() error {
	if len(c.errs) == 0 {
		return nil
	}
	return c
}
