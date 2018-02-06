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
	"fmt"
	"strconv"
	"sync"

	"github.com/bsm/sarama-cluster"
	"github.com/uber-go/kafka-client/internal/util"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type (
	// ClusterConsumer is a consumer for a single Kafka cluster.
	ClusterConsumer struct {
		cluster          string
		consumer         SaramaConsumer
		topicConsumerMap map[string]*TopicConsumer
		scope            tally.Scope
		logger           *zap.Logger
		lifecycle        *util.RunLifecycle
		stopC            chan struct{}
		doneC            chan struct{}
	}
)

// NewClusterConsumer returns a new single cluster consumer.
func NewClusterConsumer(
	cluster string,
	saramaConsumer SaramaConsumer,
	consumerMap map[string]*TopicConsumer,
	scope tally.Scope,
	logger *zap.Logger,
) *ClusterConsumer {
	return &ClusterConsumer{
		cluster:          cluster,
		consumer:         saramaConsumer,
		topicConsumerMap: consumerMap,
		scope:            scope,
		logger:           logger,
		lifecycle:        util.NewRunLifecycle(cluster + "-consumer"),
		stopC:            make(chan struct{}),
		doneC:            make(chan struct{}),
	}
}

// Start starts the consumer
func (c *ClusterConsumer) Start() error {
	return c.lifecycle.Start(func() error {
		c.logger.Info("cluster consumer starting")
		go c.eventLoop()
		return nil
	})
}

// Stop stops the consumer
func (c *ClusterConsumer) Stop() {
	c.lifecycle.Stop(func() {
		c.logger.Info("cluster consumer stopping")
		close(c.stopC)
	})
}

// Closed returns a channel which will closed after this consumer is shutdown
func (c *ClusterConsumer) Closed() <-chan struct{} {
	return c.doneC
}

// eventLoop is the main event loop for this consumer
func (c *ClusterConsumer) eventLoop() {
	c.logger.Info("consumer started")
	for {
		select {
		case pc := <-c.consumer.Partitions():
			c.addPartitionConsumer(pc)
		case n := <-c.consumer.Notifications():
			c.handleNotification(n)
		case err := <-c.consumer.Errors():
			c.logger.Error("consumer error", zap.Error(err))
		case <-c.stopC:
			c.shutdown()
			c.logger.Info("cluster consumer stopped")
			return
		}
	}
}

// addPartition adds a new partition. If the partition already exist,
// it is first stopped before overwriting it with the new partition
func (c *ClusterConsumer) addPartitionConsumer(pc cluster.PartitionConsumer) {
	topic := pc.Topic()
	topicConsumer, ok := c.topicConsumerMap[topic]
	if !ok {
		c.logger.Error("cannot consume messages for missing topic consumer", zap.String("topic", topic))
		return
	}
	topicConsumer.addPartitionConsumer(pc)
}

// handleNotification is the handler that handles notifications
// from the underlying library about partition rebalances. There
// is no action taken in this handler except for logging.
func (c *ClusterConsumer) handleNotification(n *cluster.Notification) {
	for topic, partitions := range n.Claimed {
		for _, partition := range partitions {
			c.logger.Debug("partition rebalance claimed", zap.String("topic", topic), zap.Int32("partition", partition))
		}
	}

	for topic, partitions := range n.Released {
		for _, partition := range partitions {
			c.logger.Debug("partition rebalance released", zap.String("topic", topic), zap.Int32("partition", partition))
		}
	}

	var current []string
	for topic, partitions := range n.Current {
		for _, partition := range partitions {
			current = append(current, fmt.Sprintf("%s-%s", topic, strconv.Itoa(int(partition))))
		}
	}

	c.logger.Info("owned topic-partitions", zap.Strings("topic-partitions", current))
}

// shutdown stops the consumer and frees resources
func (c *ClusterConsumer) shutdown() {
	// Close each TopicConsumer
	var wg sync.WaitGroup
	for _, tc := range c.topicConsumerMap {
		wg.Add(1)
		go func(tc *TopicConsumer) {
			tc.Stop()
			wg.Done()
		}(tc)
	}
	wg.Wait()
	c.consumer.Close() // close sarama cluster consumer
	close(c.doneC)
}
