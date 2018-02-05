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
	"github.com/Shopify/sarama"
	"github.com/uber-go/kafka-client/internal/util"
	"github.com/uber-go/kafka-client/kafka"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type (
	// MultiClusterConsumer is a map that contains multiple kafka consumers
	MultiClusterConsumer struct {
		groupName                string
		topics                   kafka.ConsumerTopicList
		clusterConsumerMap       map[string]*ClusterConsumer
		clusterToSaramaClientMap map[string]sarama.Client
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
	clusterConsumerMap map[string]*ClusterConsumer,
	saramaClients map[string]sarama.Client,
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
		lifecycle: util.NewRunLifecycle(groupName+"-consumer", logger),
	}
}

// Name returns the consumer group name used by this consumer.
func (c *MultiClusterConsumer) Name() string {
	return c.groupName
}

// Topics returns a list of topics this consumer is consuming from.
func (c *MultiClusterConsumer) Topics() []string {
	return c.topics.TopicNames()
}

// Start will fail to start if there is any clusterConsumer that fails.
func (c *MultiClusterConsumer) Start() error {
	err := c.lifecycle.Start(func() (err error) {
		for clusterName, consumer := range c.clusterConsumerMap {
			if err = consumer.Start(); err != nil {
				c.logger.With(
					zap.Error(err),
					zap.String("cluster", clusterName),
				).Error("failed to start cluster consumer")
				return
			}
		}
		return
	})
	if err != nil {
		c.Stop()
	}
	return err
}

// Stop will stop the consumer.
func (c *MultiClusterConsumer) Stop() {
	// clusterConsumers are safe to stop multiple times.
	for _, consumer := range c.clusterConsumerMap {
		consumer.Stop()
	}
	c.lifecycle.Stop(func() {
		for _, client := range c.clusterToSaramaClientMap {
			client.Close()
		}
		close(c.doneC)
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
