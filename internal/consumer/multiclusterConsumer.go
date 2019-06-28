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

package consumer

import (
	"errors"
	"github.com/Shopify/sarama"
	"github.com/gig/kafka-client/internal/metrics"
	"github.com/gig/kafka-client/internal/util"
	"github.com/gig/kafka-client/kafka"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type (
	// MultiClusterConsumer is a map that contains multiple kafka consumers
	MultiClusterConsumer struct {
		groupName                string
		topics                   kafka.ConsumerTopicList
		clusterConsumerMap       map[ClusterGroup]*ClusterConsumer
		clusterToSaramaClientMap map[ClusterGroup]sarama.Client
		msgC                     chan kafka.Message
		doneC                    chan struct{}
		scope                    tally.Scope
		logger                   *zap.Logger
		lifecycle                *util.RunLifecycle
	}
)

// NewMultiClusterConsumer returns a new consumer that consumes messages from
// multiple Kafka clusters.
func NewMultiClusterConsumer(
	groupName string,
	topics kafka.ConsumerTopicList,
	clusterConsumerMap map[ClusterGroup]*ClusterConsumer,
	saramaClients map[ClusterGroup]sarama.Client,
	msgC chan kafka.Message,
	scope tally.Scope,
	logger *zap.Logger,
) *MultiClusterConsumer {
	return &MultiClusterConsumer{
		groupName:                groupName,
		topics:                   topics,
		clusterConsumerMap:       clusterConsumerMap,
		clusterToSaramaClientMap: saramaClients,
		msgC:      msgC,
		doneC:     make(chan struct{}),
		scope:     scope,
		logger:    logger,
		lifecycle: util.NewRunLifecycle(groupName + "-consumer"),
	}
}

// Name returns the consumer group name used by this consumer.
func (c *MultiClusterConsumer) Name() string {
	return c.groupName
}

// Topics returns a list of topics this consumer is consuming from.
func (c *MultiClusterConsumer) Topics() kafka.ConsumerTopicList {
	return c.topics
}

// Start will fail to start if there is any clusterConsumer that fails.
func (c *MultiClusterConsumer) Start() error {
	err := c.lifecycle.Start(func() (err error) {
		for clusterName, consumer := range c.clusterConsumerMap {
			if err = consumer.Start(); err != nil {
				c.logger.With(
					zap.Error(err),
					zap.String("cluster", clusterName.Cluster),
				).Error("multicluster consumer start error")
				return
			}
		}
		return
	})
	if err != nil {
		c.Stop()
		return err
	}
	c.logger.Info("multicluster consumer started", zap.String("groupName", c.groupName), zap.Array("topicList", c.topics))
	c.scope.Counter(metrics.KafkaConsumerStarted).Inc(1)
	return nil
}

// Stop will stop the consumer.
func (c *MultiClusterConsumer) Stop() {
	c.lifecycle.Stop(func() {
		for _, consumer := range c.clusterConsumerMap {
			consumer.Stop()
		}
		for _, client := range c.clusterToSaramaClientMap {
			client.Close()
		}
		close(c.doneC)
		c.logger.Info("multicluster consumer stopped", zap.String("groupName", c.groupName), zap.Array("topicList", c.topics))
		c.scope.Counter(metrics.KafkaConsumerStopped).Inc(1)
	})
}

// Closed returns a channel that will be closed when the consumer is closed.
func (c *MultiClusterConsumer) Closed() <-chan struct{} {
	return c.doneC
}

// Messages returns a channel to receive messages on.
func (c *MultiClusterConsumer) Messages() <-chan kafka.Message {
	return c.msgC
}

// ResetOffset will reset the consumer offset for the specified cluster, topic, partition.
func (c *MultiClusterConsumer) ResetOffset(cluster, group, topic string, partition int32, offsetRange kafka.OffsetRange) error {
	cc, ok := c.clusterConsumerMap[ClusterGroup{Cluster: cluster, Group: group}]
	if !ok {
		return errors.New("no cluster consumer found")
	}
	return cc.ResetOffset(topic, partition, offsetRange)
}

// MergeDLQ will merge the offset range for each partition of the DLQ topic for the specified ConsumerTopic.
// Topic should be the DLQ topic (with __dlq).
func (c *MultiClusterConsumer) MergeDLQ(cluster, group, topic string, partition int32, offsetRange kafka.OffsetRange) error {
	return c.ResetOffset(cluster, group+DLQConsumerGroupNameSuffix, topic, partition, offsetRange)
}
