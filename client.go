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
	"os"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
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
	}
}

// NewConsumer returns a new instance of kafka consumer
func (c *Client) NewConsumer(config *kafka.ConsumerConfig) (kafka.Consumer, error) {
	opts := buildOptions(config)
	saramaConfig := buildSaramaConfig(&opts)
	msgCh := make(chan kafka.Message, opts.RcvBufferSize)
	topicList := c.resolveBrokers(config.TopicList)
	clusterTopicMap := topicList.ClusterTopicMap()
	dlqClusterTopicMap := topicList.DLQClusterTopicMap()

	saramaConsumerMap := c.buildSaramaConsumerMap(config.GroupName, saramaConfig, clusterTopicMap)
	saramaProducerMap := c.buildSaramaProducerMap(dlqClusterTopicMap)
	saramaClusters := c.buildSaramaClusters(saramaConsumerMap, saramaProducerMap)

	clusterConsumers := c.buildClusterConsumerMap(config, &opts, clusterTopicMap, saramaConsumerMap, saramaProducerMap, msgCh)
	return consumer.New(config, clusterConsumers, &saramaClusters, msgCh, c.tally, c.logger)
}

// buildSaramaConsumerMap takes a map of cluster to topics to consume (clusterTopicMap) from that cluster
// and returns a map of cluster to sarama consumer, where each sarama consumer will consume
// all messages from that cluster.
//
// The broker list for a cluster will be read based on the topic broker list in the first
// element of each cluster topic list passed in the clusterTopicMap argument.
func (c *Client) buildSaramaConsumerMap(
	groupName string,
	config *cluster.Config,
	clusterTopicMap map[string]kafka.ConsumerTopicList) map[string]consumer.SaramaConsumer {
	output := make(map[string]consumer.SaramaConsumer)
	for clusterName, topicList := range clusterTopicMap {
		if len(topicList) == 0 {
			c.logger.With(
				zap.String("cluster", clusterName),
			).Error("no topics to consume from this cluster")
			continue
		}

		brokers := topicList[0].BrokerList
		topicNames := topicList.TopicNames()
		cc, err := consumer.NewSaramaConsumer(brokers, groupName, topicNames, config)
		if err != nil {
			c.logger.With(
				zap.Error(err),
				zap.String("cluster", clusterName),
				zap.Strings("brokers", brokers),
				zap.Strings("topic", topicNames),
				zap.String("consumergroup", groupName),
			).Error("failed to create sarama consumer")
			continue
		}

		output[clusterName] = cc
	}
	return output
}

// buildSaramaProducerMap takes a map of dlq cluster name to list of topics to consume
// and returns a map of dlq cluster name to sarama SyncProducer.
//
// The broker list for each dlq cluster sarama SyncProducer will be inferred from the broker list
// of the first consumer topic in each ConsumerTopicList from the clusterTopicMap argument.
func (c *Client) buildSaramaProducerMap(clusterTopicMap map[string]kafka.ConsumerTopicList) map[string]sarama.SyncProducer {
	output := make(map[string]sarama.SyncProducer)
	for clusterName, topicList := range clusterTopicMap {
		if len(topicList) == 0 {
			c.logger.With(
				zap.String("cluster", clusterName),
			).Error("no topics to produce to this cluster")
			continue
		}

		brokers := topicList[0].BrokerList
		p, err := c.newSyncProducer(brokers)
		if err != nil {
			c.logger.With(
				zap.String("cluster", clusterName),
				zap.Strings("brokers", brokers),
			).Error("failed to create sarama producer for this cluster")
			continue
		}

		output[clusterName] = p
	}
	return output
}

// buildSaramaClusters builds a threadsafe SaramaClusters map based on the
// provided consumerMap and producerMap.
// For each SaramaCluster found in the output SaramaClusters map,
// the Consumer or Producer field may be null if there was no corresponding
// consumer or producer found in the provided consumerMap or producerMap.
func (c *Client) buildSaramaClusters(
	consumerMap map[string]consumer.SaramaConsumer,
	producerMap map[string]sarama.SyncProducer) consumer.SaramaClusters {
	clusters := make(map[string]*consumer.SaramaCluster)
	for clusterName, consumer := range consumerMap {
		saramaCluster, ok := clusters[clusterName]
		if !ok {
			saramaCluster = new(consumer.SaramaCluster)
		}
		saramaCluster.Consumer = consumer
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
	return consumer.SaramaClusters{
		Clusters: clusters,
	}
}

// buildDLQMap creates a map of DLQ kafka Topic to DLQ producer based on the provided topicList and producerMap.
// If a particular DLQ producer cannot be constructed, a noop DLQ producer will be used, which may result in data loss.
func (c *Client) buildDLQMap(topicList kafka.ConsumerTopicList, producerMap map[string]sarama.SyncProducer) map[string]consumer.DLQ {
	output := make(map[string]consumer.DLQ)
	for _, topic := range topicList {
		_, ok := output[topic.DLQ.HashKey()]
		if ok {
			continue
		}

		dlqProducer := consumer.NewDLQNoop()
		if producer, ok := producerMap[topic.DLQ.Cluster]; ok {
			dlqProducer = consumer.NewDLQ(
				topic.DLQ.Name,
				topic.DLQ.Cluster,
				producer,
				c.tally,
				c.logger,
			)
		} else {
			c.logger.With(
				zap.String("cluster", topic.Cluster),
				zap.String("topic", topic.Name),
				zap.String("dlqCluster", topic.DLQ.Cluster),
				zap.String("dlqTopic", topic.DLQ.Name),
			).Error("failed to get sarama producer so using noop DLQ producer, which may result in data loss")
		}

		output[topic.DLQ.HashKey()] = dlqProducer
	}
	return output
}

// buildClusterConsumerMap returns a map of kafka ClusterConsumer per cluster to consume as provided by the consumerMap argument.
func (c *Client) buildClusterConsumerMap(
	config *kafka.ConsumerConfig,
	options *consumer.Options,
	clusterTopicMap map[string]kafka.ConsumerTopicList,
	consumerMap map[string]consumer.SaramaConsumer,
	producerMap map[string]sarama.SyncProducer,
	msgCh chan kafka.Message) map[string]kafka.Consumer {
	output := make(map[string]kafka.Consumer)
	for clusterName, topicList := range clusterTopicMap {
		sc, ok := consumerMap[clusterName]
		if !ok {
			c.logger.With(
				zap.String("cluster", clusterName),
			).Error("failed to find sarama consumer for cluster so will not consume this cluster")
			continue
		}

		dlqProducers := c.buildDLQMap(topicList, producerMap)
		consumer, err := consumer.NewClusterConsumer(
			clusterName,
			config,
			options,
			topicList,
			msgCh,
			sc,
			dlqProducers,
			c.tally,
			c.logger,
		)
		if err != nil {
			c.logger.With(
				zap.String("cluster", clusterName),
			).Error("failed to create cluster consumer so will not consume from this cluster")
		}

		output[clusterName] = consumer
	}
	return output
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
			brokers, err := c.resolver.ResolveIPForCluster(topic.Cluster)
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
func (c *Client) newSyncProducer(brokers []string) (sarama.SyncProducer, error) {
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
