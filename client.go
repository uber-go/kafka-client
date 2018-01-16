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
type Client struct {
	tally    tally.Scope
	logger   *zap.Logger
	resolver kafka.NameResolver

	clusterConsumerConstructor    func(string, *kafka.ConsumerConfig, *consumer.Options, kafka.ConsumerTopicList, chan kafka.Message, consumer.SaramaConsumer, map[string]consumer.DLQ, tally.Scope, *zap.Logger) (kafka.Consumer, error)
	saramaSyncProducerConstructor func([]string) (sarama.SyncProducer, error)
	saramaConsumerConstructor     func([]string, string, []string, *cluster.Config) (consumer.SaramaConsumer, error)
}

var defaultOptions = consumer.Options{
	Concurrency:            1024,
	RcvBufferSize:          2 * 1024, // twice the concurrency for compute/io overlap
	PartitionRcvBufferSize: 32,
	OffsetCommitInterval:   time.Second,
	RebalanceDwellTime:     time.Second,
	MaxProcessingTime:      250 * time.Millisecond,
	OffsetPolicy:           sarama.OffsetOldest,
	ConsumerMode:           cluster.ConsumerModePartitions,
}

// New returns a new kafka client
func New(resolver kafka.NameResolver, logger *zap.Logger, scope tally.Scope) kafka.Client {
	return &Client{
		resolver: resolver,
		logger:   logger,
		tally:    scope,
		clusterConsumerConstructor:    consumer.NewClusterConsumer,
		saramaSyncProducerConstructor: newSyncProducer,
		saramaConsumerConstructor:     consumer.NewSaramaConsumer,
	}
}

// NewConsumer returns a new instance of kafka consumer
func (c *Client) NewConsumer(config *kafka.ConsumerConfig) (kafka.Consumer, error) {
	opts := buildOptions(config)
	msgCh := make(chan kafka.Message, opts.RcvBufferSize)
	topicList := c.resolveBrokers(config.TopicList)
	clusterConsumerMap, saramaProducerMap, saramaConsumerMap := c.buildClusterConsumerMap(config, &opts, msgCh, topicList)
	saramaClusters := c.buildSaramaClusters(saramaConsumerMap, saramaProducerMap)
	return consumer.New(config, clusterConsumerMap, saramaClusters, msgCh, c.tally, c.logger)
}

// buildSaramaClusters builds a threadsafe SaramaClusters map based on the
// provided consumerMap and producerMap.
// For each SaramaCluster found in the input SaramaClusters map,
// the Consumer or Producer field may be null if there was no corresponding
// consumer or producer found in the provided consumerMap or producerMap.
func (c *Client) buildSaramaClusters(
	consumerMap map[string]consumer.SaramaConsumer,
	producerMap map[string]sarama.SyncProducer) map[string]*consumer.SaramaCluster {
	clusters := make(map[string]*consumer.SaramaCluster)
	for clusterName, sc := range consumerMap {
		saramaCluster, ok := clusters[clusterName]
		if !ok {
			saramaCluster = new(consumer.SaramaCluster)
		}
		saramaCluster.Consumer = sc
		clusters[clusterName] = saramaCluster
	}

	for clusterName, producer := range producerMap {
		saramaCluster, ok := clusters[clusterName]
		if !ok {
			saramaCluster = new(consumer.SaramaCluster)
		}
		saramaCluster.Producer = producer
		clusters[clusterName] = saramaCluster
	}
	return clusters
}

// buildClusterConsumerMap returns a map of kafka ClusterConsumer per cluster to consume as provided by the consumerMap argument.
func (c *Client) buildClusterConsumerMap(
	config *kafka.ConsumerConfig,
	opts *consumer.Options,
	msgCh chan kafka.Message,
	topicList kafka.ConsumerTopicList) (map[string]kafka.Consumer, map[string]sarama.SyncProducer, map[string]consumer.SaramaConsumer) {
	saramaConfig := buildSaramaConfig(opts)
	saramaProducerMap := make(map[string]sarama.SyncProducer)
	saramaConsumerMap := make(map[string]consumer.SaramaConsumer)

	clusterTopicMap := topicList.ClusterTopicMap()
	clusterConsumerMap := make(map[string]kafka.Consumer)

	for clusterName, clusterTopicList := range clusterTopicMap {
		cc, err := c.buildClusterConsumer(
			config,
			opts,
			saramaConfig,
			saramaConsumerMap,
			saramaProducerMap,
			msgCh,
			c.tally,
			c.logger,
			clusterName,
			clusterTopicList,
		)
		if err != nil {
			c.logger.With(
				zap.Error(err),
				zap.String("cluster", clusterName),
			).Error("will not consume from cluster")
			continue
		}
		clusterConsumerMap[clusterName] = cc
	}

	return clusterConsumerMap, saramaProducerMap, saramaConsumerMap
}
func (c *Client) buildClusterConsumer(
	config *kafka.ConsumerConfig,
	options *consumer.Options,
	saramaConfig *cluster.Config,
	saramaConsumerMap map[string]consumer.SaramaConsumer,
	saramaProducerMap map[string]sarama.SyncProducer,
	msgCh chan kafka.Message,
	tally tally.Scope,
	log *zap.Logger,
	clusterName string,
	clusterTopicList kafka.ConsumerTopicList) (kafka.Consumer, error) {
	var err error
	dlqMap := make(map[string]consumer.DLQ)
	var topicList kafka.ConsumerTopicList = make([]kafka.ConsumerTopic, 0, len(clusterTopicList))

	// Create DLQ producer for each topic to consume with DLQ set.
	// If unable to create DLQ, do not consume that topic
	for _, consumerTopic := range clusterTopicList {
		if consumerTopic.DLQ.Name != "" && consumerTopic.DLQ.Cluster != "" && len(consumerTopic.DLQ.BrokerList) > 0 {
			sp, ok := saramaProducerMap[consumerTopic.DLQ.Cluster]
			if !ok {
				sp, err = c.saramaSyncProducerConstructor(consumerTopic.DLQ.BrokerList)
				if err != nil {
					log.With(
						zap.Error(err),
					).Error("will not consume topic")
					continue
				}
			}
			saramaProducerMap[consumerTopic.DLQ.Cluster] = sp

			dlq := consumer.NewDLQ(consumerTopic.DLQ.Name, consumerTopic.DLQ.Cluster, sp, tally, log)
			dlqMap[consumerTopic.DLQ.HashKey()] = dlq
			topicList = append(topicList, consumerTopic)
		}
	}

	if len(topicList) == 0 {
		return nil, fmt.Errorf("no topics to consume on this cluster")
	}
	// Assumption: all ConsumerTopics in ClusterTopicList are from the same cluster
	// so we use the first one to retrieve the broker list.
	brokers := clusterTopicList[0].BrokerList
	sc, err := c.saramaConsumerConstructor(brokers, config.GroupName, topicList.TopicNames(), saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create sarama consumer for this cluster")
	}
	saramaConsumerMap[clusterName] = sc

	return c.clusterConsumerConstructor(
		clusterName,
		config,
		options,
		topicList,
		msgCh,
		sc,
		dlqMap,
		tally,
		log,
	)
}

// resolveBrokers will attempt to resolve BrokerList for each topic in the inputTopicList.
// If the broker list cannot be resolved, the topic will be removed from the outputTopicList so that
// the consumer will not consume that topic.
func (c *Client) resolveBrokers(inputTopicList kafka.ConsumerTopicList) kafka.ConsumerTopicList {
	outputTopicList := make([]kafka.ConsumerTopic, 0, len(inputTopicList))
	for _, topic := range inputTopicList {
		if len(topic.BrokerList) == 0 {
			brokers, err := c.resolver.ResolveIPForCluster(topic.Cluster)
			if err != nil {
				c.logger.With(
					zap.Error(err),
					zap.String("topic", topic.Name),
					zap.String("cluster", topic.Cluster),
				).Error("failed to resolve brokers IP so will not consume this topic.")
				continue
			}
			topic.BrokerList = brokers
		}

		if topic.DLQ.Name != "" && len(topic.DLQ.BrokerList) == 0 {
			brokers, err := c.resolver.ResolveIPForCluster(topic.DLQ.Cluster)
			if err != nil {
				c.logger.With(
					zap.Error(err),
					zap.String("dlqTopic", topic.DLQ.Name),
					zap.String("dlqCluster", topic.DLQ.Cluster),
					zap.String("topic", topic.Name),
					zap.String("cluster", topic.Cluster),
				).Error("failed to resolve brokers DLQ cluster IP so will not consume this topic.")
				continue
			}
			topic.DLQ.BrokerList = brokers
		}

		outputTopicList = append(outputTopicList, topic)
	}
	return outputTopicList
}

// newSyncProducer returns a new sarama sync producer from the cluster config
func newSyncProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Flush.Frequency = time.Millisecond * 500
	return consumer.NewSaramaProducer(brokers, config)
}

func buildOptions(config *kafka.ConsumerConfig) consumer.Options {
	opts := defaultOptions
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

func clientID() string {
	name, err := os.Hostname()
	if err != nil {
		name = "unknown-kafka-client"
	}
	return name
}
