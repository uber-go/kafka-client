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

	"os"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/uber-go/kafka-client/internal/consumer"
	"github.com/uber-go/kafka-client/kafka"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"fmt"
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
	saramaConsumerMap := c.saramaConsumerMap(config)
	saramaProducerMap := c.saramaProducerMap(config)
	return consumer.New(config, saramaConsumerMap, saramaProducerMap, c.tally, c.logger)
}

func (c *Client) saramaProducerMap(config *kafka.ConsumerConfig) map[string]sarama.SyncProducer {
	output := make(map[string]sarama.SyncProducer)
	clusterTopicMap := c.clusterTopicMap(config)
	for clusterName, _ := range clusterTopicMap {
		brokers, err := c.resolver.ResolveIPForCluster(clusterName)
		if err != nil {
			c.logger.With(zap.Error(err)).Error(fmt.Sprintf("Failed to resolve broker IP for cluster %s", clusterName))
			continue
		}
		producer, err := c.newSyncProducer(brokers)
		if err != nil {
			c.logger.With(zap.Error(err)).Error(fmt.Sprintf("Failed to create sarama SyncProducer for cluster %s", clusterName))
			continue
		}
		output[clusterName] = producer
	}
	return output
}

func (c *Client) saramaConsumerMap(config *kafka.ConsumerConfig) map[string]consumer.SaramaConsumer {
	clusterTopicMap := c.clusterTopicMap(config)
	opts := buildOptions(config)
	saramaConfig := buildSaramaConfig(&opts)

	output := make(map[string]consumer.SaramaConsumer)
	for clusterName, topicInfo := range clusterTopicMap {
		brokers, err := c.resolver.ResolveIPForCluster(clusterName)
		if err != nil {
			c.logger.With(zap.Error(err)).Error(fmt.Sprintf("Failed to resolve broker IP for %s", clusterName))
			continue
		}

		saramaConsumer, err := cluster.NewConsumer(brokers, config.GroupName, topicInfo.TopicsAsString(), saramaConfig)
		if err != nil {
			c.logger.With(zap.Error(err)).Error(fmt.Sprintf("Failed to create sarama consumer"))
			continue
		}

		output[clusterName] = saramaConsumer
	}
	return output
}

func (c *Client) clusterTopicMap(config *kafka.ConsumerConfig) map[string]kafka.Topics{
	output := make(map[string]kafka.Topics)
	for _, info := range config.Topics {
		cluster := info.Cluster
		infos, ok := output[cluster]
		if !ok {
			infos = make([]kafka.TopicConfig, 0)
		}
		infos = append(infos, info)
		output[cluster] = infos
	}
	return output
}

// newSyncProducer returns a new sarama sync producer from the cluster config
func (c *Client) newSyncProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Flush.Frequency = time.Millisecond * 500
	return sarama.NewSyncProducer(brokers, config)
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
