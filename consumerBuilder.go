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
		config *kafka.ConsumerConfig

		clusterSaramaClientMap  map[string]sarama.Client
		clusterTopicConsumerMap map[string]map[string]*consumer.TopicConsumer
		clusterConsumerMap      map[string]*consumer.ClusterConsumer

		msgCh        chan kafka.Message
		logger       *zap.Logger
		scope        tally.Scope
		constructors consumer.Constructors
		resolver     kafka.NameResolver

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
		config:                  config,
		clusterSaramaClientMap:  make(map[string]sarama.Client),
		clusterTopicConsumerMap: make(map[string]map[string]*consumer.TopicConsumer),
		clusterConsumerMap:      make(map[string]*consumer.ClusterConsumer),
		msgCh:                   make(chan kafka.Message, consumerOptions.RcvBufferSize),
		logger:                  logger,
		scope:                   scope,
		constructors: consumer.Constructors{
			NewSaramaConsumer: consumer.NewSaramaConsumer,
			NewSaramaProducer: consumer.NewSaramaProducer,
			NewSaramaClient:   consumer.NewSaramaClient,
		},
		resolver:            resolver,
		options:             consumerOptions,
		saramaConfig:        &saramaClusterConfig.Config,
		saramaClusterConfig: saramaClusterConfig,
	}
}

func (c *consumerBuilder) build() (kafka.Consumer, error) {
	// build Topic Consumer for each consumer topic
	for _, topic := range c.config.TopicList {
		// Get or make DLQ topic
		// (Get or make Retry topic here too)
		dlq, err := c.getOrAddDLQ(topic.DLQ)
		if err != nil {
			return nil, err
		}

		// Get or make cluster consumer
		// (Get or make Retry and DLQ topic here too)
		topicConsumerMap, ok := c.clusterTopicConsumerMap[topic.Topic.Cluster]
		if !ok {
			topicConsumerMap = make(map[string]*consumer.TopicConsumer)
		}
		topicConsumerMap[topic.Name] = consumer.NewTopicConsumer(
			topic.Name,
			c.msgCh,
			dlq,
			c.options,
			c.scope,
			c.logger,
		)
		c.clusterTopicConsumerMap[topic.Topic.Cluster] = topicConsumerMap
	}

	// build cluster consumers
	for cluster, topicConsumers := range c.clusterTopicConsumerMap {
		// get topic list to consume
		topicList := make([]string, 0, len(topicConsumers))
		for topic := range topicConsumers {
			topicList = append(topicList, topic)
		}

		// get broker list
		brokerList, err := c.resolver.ResolveIPForCluster(cluster)
		if err != nil {
			return nil, err
		}

		// make new sarama consumer
		saramaConsumer, err := c.constructors.NewSaramaConsumer(brokerList, c.config.GroupName, topicList, c.saramaClusterConfig)
		if err != nil {
			return nil, err
		}

		// make cluster consumer
		c.clusterConsumerMap[cluster] = consumer.NewClusterConsumer(
			cluster,
			saramaConsumer,
			topicConsumers,
			c.scope,
			c.logger,
		)

		// pass reference sarama consumer to topic consumers
		for _, topicConsumer := range topicConsumers {
			topicConsumer.SetSaramaConsumer(saramaConsumer)
		}
	}

	// make multi cluster consumer
	return consumer.NewMultiClusterConsumer(
		c.config.GroupName,
		c.config.TopicList,
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
