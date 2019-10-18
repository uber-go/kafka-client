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
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/uber-go/kafka-client/internal/consumer"
	"github.com/uber-go/kafka-client/kafka"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type (
	consumerBuilder struct {
		clusterTopicsMap map[consumerCluster][]consumer.Topic

		clusterSaramaClientMap        map[consumer.ClusterGroup]sarama.Client
		clusterSaramaConsumerMap      map[consumer.ClusterGroup]consumer.SaramaConsumer
		clusterTopicSaramaProducerMap map[string]map[string]sarama.AsyncProducer

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

	// consumerCluster wraps a consumer cluster name and relevant config for consuming from that cluster.
	consumerCluster struct {
		// name is the name of the cluster.
		name string
		// initialOffset is the initial offset of the cluster if there is no previously checkpointed offset.
		initialOffset int64
		// group name to use when consuming from this cluster
		groupName string
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
		clusterTopicsMap:              make(map[consumerCluster][]consumer.Topic),
		clusterSaramaClientMap:        make(map[consumer.ClusterGroup]sarama.Client),
		clusterSaramaConsumerMap:      make(map[consumer.ClusterGroup]consumer.SaramaConsumer),
		clusterTopicSaramaProducerMap: make(map[string]map[string]sarama.AsyncProducer),
		msgCh:                         make(chan kafka.Message, consumerOptions.RcvBufferSize),
		logger:                        logger.With(zap.String("consumergroup", config.GroupName)),
		scope:                         scope.Tagged(map[string]string{"consumergroup": config.GroupName}),
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

func (c *consumerBuilder) addTopicToClusterTopicsMap(topic consumer.Topic, offsetPolicy int64) {
	groupName := c.kafkaConfig.GroupName + topic.ConsumerGroupSuffix
	topicList, ok := c.clusterTopicsMap[consumerCluster{
		name:          topic.Topic.Cluster,
		initialOffset: offsetPolicy,
		groupName:     groupName,
	}]
	if !ok {
		topicList = make([]consumer.Topic, 0, 10)
	}
	topicList = append(topicList, topic)
	c.clusterTopicsMap[consumerCluster{
		name:          topic.Topic.Cluster,
		initialOffset: offsetPolicy,
		groupName:     groupName,
	}] = topicList
}

func (c *consumerBuilder) topicConsumerBuilderToTopicNames(topicList []consumer.Topic) []string {
	output := make([]string, 0, len(topicList))
	for _, topic := range topicList {
		output = append(output, topic.Name)
	}
	return output
}

func (c *consumerBuilder) Build() (kafka.Consumer, error) {
	return c.build()
}

func (c *consumerBuilder) build() (*consumer.MultiClusterConsumer, error) {
	// build TopicList per cluster
	for _, consumerTopic := range c.kafkaConfig.TopicList {
		// first, add TopicConsumer for original topic if topic is well defined.
		// disabling offset commit only applies for the original topic.
		if consumerTopic.Topic.Name != "" && consumerTopic.Topic.Cluster != "" {
			partitionConsumerFactory := consumer.NewPartitionConsumer
			if !c.kafkaConfig.Offsets.Commits.Enabled {
				partitionConsumerFactory = consumer.NewPartitionConsumerWithoutCommit
			}
			c.addTopicToClusterTopicsMap(consumer.Topic{ConsumerTopic: consumerTopic, DLQMetadataDecoder: consumer.NoopDLQMetadataDecoder, PartitionConsumerFactory: partitionConsumerFactory}, c.kafkaConfig.Offsets.Initial.Offset)
		}
		// Second, add retryQ topic if enabled.
		if consumerTopic.RetryQ.Name != "" && consumerTopic.RetryQ.Cluster != "" {
			c.addTopicToClusterTopicsMap(consumer.Topic{ConsumerTopic: topicToRetryTopic(consumerTopic), DLQMetadataDecoder: consumer.ProtobufDLQMetadataDecoder, PartitionConsumerFactory: consumer.NewPartitionConsumer}, sarama.OffsetOldest)
		}
		// Third, add DLQ topic if enabled.
		if consumerTopic.DLQ.Name != "" && consumerTopic.DLQ.Cluster != "" {
			c.addTopicToClusterTopicsMap(consumer.Topic{ConsumerTopic: topicToDLQTopic(consumerTopic), DLQMetadataDecoder: consumer.ProtobufDLQMetadataDecoder, PartitionConsumerFactory: consumer.NewRangePartitionConsumer, ConsumerGroupSuffix: consumer.DLQConsumerGroupNameSuffix}, sarama.OffsetOldest)
		}
	}

	// Add additional topics that may have been injected from WithRangeConsumer option.
	for _, topic := range c.options.OtherConsumerTopics {
		c.addTopicToClusterTopicsMap(topic, sarama.OffsetOldest) // OtherConsumerTopics are retry or dlq so default to offset oldest.
	}

	// build cluster consumer
	clusterConsumerMap := make(map[consumer.ClusterGroup]*consumer.ClusterConsumer)
	for cluster, topicList := range c.clusterTopicsMap {
		uniqueTopicList := c.uniqueTopics(topicList)
		saramaConsumer, err := c.getOrAddSaramaConsumer(cluster, uniqueTopicList)
		if err != nil {
			c.close()
			return nil, err
		}

		// build topic consumers
		topicConsumerMap := make(map[string]*consumer.TopicConsumer)
		for _, topic := range uniqueTopicList {
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
			topicConsumerMap[topic.Name] = topicConsumer
		}
		clusterConsumerMap[consumer.ClusterGroup{
			Cluster: cluster.name,
			Group:   cluster.groupName,
		}] = consumer.NewClusterConsumer(
			cluster.name,
			saramaConsumer,
			topicConsumerMap,
			c.scope,
			c.logger,
		)
	}

	// make multi cluster consumer
	return consumer.NewMultiClusterConsumer(
		c.kafkaConfig.GroupName,
		c.kafkaConfig.TopicList,
		clusterConsumerMap,
		c.clusterSaramaClientMap,
		c.msgCh,
		c.scope,
		c.logger,
	), nil
}

func (c *consumerBuilder) getOrAddDLQ(topic kafka.Topic) (consumer.DLQ, error) {
	if topic.Cluster == "" {
		return consumer.NewNoopDLQ(), nil
	}

	sp, err := c.getOrAddSaramaProducer(topic)
	if err != nil {
		return nil, err
	}
	return consumer.NewBufferedDLQ(topic, sp, c.scope, c.logger), nil
}

func (c *consumerBuilder) getOrAddSaramaProducer(topic kafka.Topic) (sarama.AsyncProducer, error) {
	var err error
	sc, err := c.getOrAddSaramaClient(topic)
	if err != nil {
		return nil, err
	}

	topicSaramaProducerMap, ok := c.clusterTopicSaramaProducerMap[topic.Cluster]
	if !ok {
		topicSaramaProducerMap = make(map[string]sarama.AsyncProducer)
	}
	sp, ok := topicSaramaProducerMap[topic.Name]
	if !ok {
		sp, err = c.constructors.NewSaramaProducer(sc)
		if err != nil {
			return nil, err
		}
	}
	topicSaramaProducerMap[topic.Name] = sp
	c.clusterTopicSaramaProducerMap[topic.Cluster] = topicSaramaProducerMap
	return sp, nil
}

// getOrAddSaramaClient is only used in producer.
func (c *consumerBuilder) getOrAddSaramaClient(topic kafka.Topic) (sarama.Client, error) {
	var err error
	sc, ok := c.clusterSaramaClientMap[consumer.ClusterGroup{Cluster: topic.Cluster, Group: ""}]
	if !ok {
		var brokerList []string
		brokerList, err = c.resolver.ResolveIPForCluster(topic.Cluster)
		if err != nil {
			return nil, err
		}
		sc, err = c.constructors.NewSaramaClient(brokerList, c.saramaConfig)
		if err != nil {
			return nil, err
		}
	}
	c.clusterSaramaClientMap[consumer.ClusterGroup{Cluster: topic.Cluster, Group: ""}] = sc
	return sc, nil
}

func (c *consumerBuilder) close() {
	for _, sarama := range c.clusterSaramaClientMap {
		sarama.Close()
	}
	for _, sarama := range c.clusterSaramaConsumerMap {
		sarama.Close()
	}
	for _, topicMap := range c.clusterTopicSaramaProducerMap {
		for _, sarama := range topicMap {
			sarama.Close()
		}
	}
}

func (c *consumerBuilder) getOrAddSaramaConsumer(cluster consumerCluster, topicList []consumer.Topic) (consumer.SaramaConsumer, error) {
	brokerList, err := c.resolver.ResolveIPForCluster(cluster.name)
	if err != nil {
		return nil, err
	}

	saramaConfig := *c.saramaClusterConfig
	saramaConfig.Consumer.Offsets.Initial = cluster.initialOffset

	saramaConsumer, err := c.constructors.NewSaramaConsumer(
		brokerList,
		cluster.groupName,
		c.topicConsumerBuilderToTopicNames(topicList),
		&saramaConfig,
	)
	if err != nil {
		return nil, err
	}

	c.clusterSaramaConsumerMap[consumer.ClusterGroup{Cluster: cluster.name, Group: cluster.groupName}] = saramaConsumer
	return saramaConsumer, nil
}

func (c *consumerBuilder) uniqueTopics(topics []consumer.Topic) []consumer.Topic {
	topicSet := make(map[string]bool)
	uniqueTopics := make([]consumer.Topic, 0, len(topics))
	for _, topic := range topics {
		_, ok := topicSet[topic.Name]
		if !ok {
			topicSet[topic.Name] = false
			uniqueTopics = append(uniqueTopics, topic)
		}
	}
	return uniqueTopics
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

	// Copy the TLS config
	opts.TLSConfig = config.TLSConfig

	return opts
}

func buildSaramaConfig(options *consumer.Options) *cluster.Config {
	config := cluster.NewConfig()
	config.ClientID = clientID()
	if options.ClientID != "" {
		config.ClientID = options.ClientID
	}
	// TODO: make a cluster-wise kafkaVersion assignment. this hard coded assignment is used for enabling dlq consumer message timestamp injection.
	config.Config.Version = sarama.V0_10_2_0
	config.Config.Net.TLS.Enable = options.TLSConfig != nil
	config.Config.Net.TLS.Config = options.TLSConfig
	config.Config.Producer.MaxMessageBytes = options.ProducerMaxMessageByes
	config.Config.Producer.RequiredAcks = sarama.WaitForAll
	config.Config.Producer.Return.Successes = true
	config.Config.Producer.Return.Errors = true
	config.Config.Producer.Flush.Messages = options.Concurrency - 1 // one less than concurrency to guarantee flush
	config.Config.Producer.Flush.Frequency = 1 * time.Second
	config.ChannelBufferSize = options.PartitionRcvBufferSize
	config.Group.Mode = options.ConsumerMode
	config.Group.Return.Notifications = true
	config.Group.Offsets.Synchronization.DwellTime = options.RebalanceDwellTime
	config.Consumer.Fetch.Default = options.FetchDefaultBytes
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

func topicToDLQTopic(topic kafka.ConsumerTopic) kafka.ConsumerTopic {
	return kafka.ConsumerTopic{
		Topic:      topic.DLQ,
		RetryQ:     topic.DLQ,
		DLQ:        topic.DLQ,
		MaxRetries: 0,
	}
}
