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
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/uber-go/kafka-client/internal/consumer"
	"github.com/uber-go/kafka-client/kafka"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type (
	consumerBuilder struct {
		clusterTopicsMap         map[string][]consumer.Topic
		clusterSaramaClientMap   map[string]sarama.Client
		clusterSaramaConsumerMap map[string]consumer.SaramaConsumer
		clusterTopicConsumerMap  map[string]map[string]*consumer.TopicConsumer
		clusterConsumerMap       map[string]*consumer.ClusterConsumer

		msgCh  chan kafka.Message
		logger *zap.Logger
		scope  tally.Scope

		constructors consumer.Constructors
		resolver     kafka.NameResolver

		kafkaConfig         *kafka.ConsumerConfig
		options             *consumer.Options
		saramaConfig        *sarama.Config
		saramaClusterConfig *cluster.Config
	}
)

func newConsumerBuilder(
	config *kafka.ConsumerConfig,
	resolver kafka.NameResolver,
	scope tally.Scope,
	logger *zap.Logger,
	opts ...ConsumerOption) *consumerBuilder {
	consumerOptions := buildOptions(config, opts...)
	saramaClusterConfig := buildSaramaConfig(consumerOptions)
	return &consumerBuilder{
		clusterTopicsMap:         make(map[string][]consumer.Topic),
		clusterSaramaClientMap:   make(map[string]sarama.Client),
		clusterSaramaConsumerMap: make(map[string]consumer.SaramaConsumer),
		clusterTopicConsumerMap:  make(map[string]map[string]*consumer.TopicConsumer),
		clusterConsumerMap:       make(map[string]*consumer.ClusterConsumer),
		msgCh:                    make(chan kafka.Message, consumerOptions.RcvBufferSize),
		logger:                   logger,
		scope:                    scope,
		constructors: consumer.Constructors{
			NewSaramaConsumer: consumer.NewSaramaConsumer,
			NewSaramaProducer: consumer.NewSaramaProducer,
			NewSaramaClient:   consumer.NewSaramaClient,
		},
		resolver:            resolver,
		options:             consumerOptions,
		kafkaConfig:         config,
		saramaConfig:        &saramaClusterConfig.Config,
		saramaClusterConfig: saramaClusterConfig,
	}
}

func (c *consumerBuilder) addTopicToClusterTopicsMap(topic consumer.Topic) {
	topicList, ok := c.clusterTopicsMap[topic.Topic.Cluster]
	if !ok {
		topicList = make([]consumer.Topic, 0, 10)
	}
	topicList = append(topicList, topic)
	c.clusterTopicsMap[topic.Topic.Cluster] = topicList
}

func (c *consumerBuilder) topicConsumerBuilderToTopicNames(topicList []consumer.Topic) []string {
	output := make([]string, 0, len(topicList))
	for _, topic := range topicList {
		output = append(output, topic.Name)
	}
	return output
}

func (c *consumerBuilder) build() (kafka.Consumer, error) {
	// build TopicList per cluster
	for _, consumerTopic := range c.kafkaConfig.TopicList {
		c.addTopicToClusterTopicsMap(consumer.Topic{ConsumerTopic: consumerTopic, TopicType: consumer.TopicTypeDefaultQ})
		c.addTopicToClusterTopicsMap(consumer.Topic{ConsumerTopic: topicToRetryTopic(consumerTopic), TopicType: consumer.TopicTypeRetryQ})
	}

	// build cluster consumer
	for cluster, topicList := range c.clusterTopicsMap {
		saramaConsumer, err := c.getOrAddSaramaConsumer(cluster, topicList)
		if err != nil {
			c.close()
			return nil, err
		}

		// build topic consumers
		for _, topic := range topicList {
			retry, err := c.getOrAddDLQ(topic.RetryQ)
			if err != nil {
				c.close()
				return nil, err
			}

			dlq, err := c.getOrAddDLQ(topic.DLQ)
			if err != nil {
				c.close()
				return nil, err
			}

			retryDLQMultiplexer := consumer.NewRetryDLQMultiplexer(retry, dlq, topic.MaxRetries)
			topicConsumer := consumer.NewTopicConsumer(
				topic,
				c.msgCh,
				saramaConsumer,
				retryDLQMultiplexer,
				c.options,
				c.scope,
				c.logger,
			)
			c.addTopicConsumer(cluster, topic.Name, topicConsumer)
		}

		c.clusterConsumerMap[cluster] = consumer.NewClusterConsumer(
			cluster,
			saramaConsumer,
			c.clusterTopicConsumerMap[cluster],
			c.scope,
			c.logger,
		)
	}

	// make multi cluster consumer
	return consumer.NewMultiClusterConsumer(
		c.kafkaConfig.GroupName,
		c.kafkaConfig.TopicList,
		c.clusterConsumerMap,
		c.clusterSaramaClientMap,
		c.msgCh,
		c.scope,
		c.logger,
	), nil
}

func (c *consumerBuilder) getOrAddDLQ(topic kafka.Topic) (consumer.DLQ, error) {
	sc, err := c.getOrAddSaramaClient(topic)
	if err != nil {
		return nil, err
	}
	dlq, err := consumer.NewDLQ(topic.Name, topic.Cluster, sc, c.scope, c.logger)
	if err != nil {
		return nil, err
	}
	return dlq, nil
}

func (c *consumerBuilder) getOrAddSaramaClient(topic kafka.Topic) (sarama.Client, error) {
	var err error
	sc, ok := c.clusterSaramaClientMap[topic.Cluster]
	if !ok {
		sc, err = c.constructors.NewSaramaClient(topic.BrokerList, c.saramaConfig)
		if err != nil {
			return nil, err
		}
	}
	c.clusterSaramaClientMap[topic.Cluster] = sc
	return sc, nil
}

func (c *consumerBuilder) close() {
	for _, sarama := range c.clusterSaramaClientMap {
		sarama.Close()
	}
	for _, sarama := range c.clusterSaramaConsumerMap {
		sarama.Close()
	}
}

func (c *consumerBuilder) getOrAddSaramaConsumer(cluster string, topicList []consumer.Topic) (consumer.SaramaConsumer, error) {
	brokerList, err := c.resolver.ResolveIPForCluster(cluster)
	if err != nil {
		return nil, err
	}

	saramaConsumer, err := c.constructors.NewSaramaConsumer(
		brokerList,
		c.kafkaConfig.GroupName,
		c.topicConsumerBuilderToTopicNames(topicList),
		c.saramaClusterConfig,
	)
	if err != nil {
		return nil, err
	}

	c.clusterSaramaConsumerMap[cluster] = saramaConsumer
	return saramaConsumer, nil
}

func (c *consumerBuilder) addTopicConsumer(cluster, topic string, tc *consumer.TopicConsumer) {
	topicConsumerMap, ok := c.clusterTopicConsumerMap[cluster]
	if !ok {
		topicConsumerMap = make(map[string]*consumer.TopicConsumer)
	}
	topicConsumerMap[topic] = tc
	c.clusterTopicConsumerMap[cluster] = topicConsumerMap
}

func buildOptions(config *kafka.ConsumerConfig, consumerOpts ...ConsumerOption) *consumer.Options {
	opts := consumer.DefaultOptions()
	if config.Concurrency > 0 {
		opts.Concurrency = config.Concurrency
		opts.RcvBufferSize = 2 * opts.Concurrency
	}
	offsetPolicy := config.Offsets.Initial.Offset
	if offsetPolicy == sarama.OffsetNewest || offsetPolicy == sarama.OffsetOldest {
		opts.OffsetPolicy = config.Offsets.Initial.Offset
	}

	// Apply optional consumer parameters that may be passed in.
	for _, cOpt := range consumerOpts {
		cOpt.apply(opts)
	}

	return opts
}

func buildSaramaConfig(options *consumer.Options) *cluster.Config {
	config := cluster.NewConfig()
	config.ClientID = clientID()
	config.Config.Producer.RequiredAcks = sarama.WaitForAll
	config.Config.Producer.Return.Successes = true
	config.Config.Producer.Return.Errors = true
	config.Config.Producer.Flush.Messages = options.Concurrency
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

func topicToRetryTopic(topic kafka.ConsumerTopic) kafka.ConsumerTopic {
	return kafka.ConsumerTopic{
		Topic:      topic.RetryQ,
		RetryQ:     topic.RetryQ,
		DLQ:        topic.DLQ,
		MaxRetries: topic.MaxRetries,
	}
}
