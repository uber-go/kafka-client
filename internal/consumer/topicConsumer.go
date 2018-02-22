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
	"sync"

	"github.com/bsm/sarama-cluster"
	"github.com/uber-go/kafka-client/kafka"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type (
	// TopicConsumer is a consumer for a specific topic.
	// TopicConsumer is an abstraction that runs on the same
	// goroutine as the cluster consumer.
	TopicConsumer struct {
		topic                Topic
		msgC                 chan kafka.Message
		partitionConsumerMap map[int32]PartitionConsumer
		dlq                  DLQ
		saramaConsumer       SaramaConsumer // SaramaConsumer is a shared resource that is owned by ClusterConsumer.
		options              *Options
		scope                tally.Scope
		logger               *zap.Logger
	}
)

// NewTopicConsumer returns a new TopicConsumer for consuming from a single topic.
func NewTopicConsumer(
	topic Topic,
	msgC chan kafka.Message,
	consumer SaramaConsumer,
	dlq DLQ,
	options *Options,
	scope tally.Scope,
	logger *zap.Logger,
) *TopicConsumer {
	return &TopicConsumer{
		topic:                topic,
		msgC:                 msgC,
		partitionConsumerMap: make(map[int32]PartitionConsumer),
		dlq:                  dlq,
		saramaConsumer:       consumer,
		options:              options,
		scope:                scope,
		logger:               logger.With(zap.String("topic", topic.Name), zap.String("cluster", topic.Cluster)),
	}
}

// Start the DLQ consumer goroutine.
func (c *TopicConsumer) Start() error {
	if err := c.dlq.Start(); err != nil {
		c.logger.Error("topic consumer start error", zap.Error(err))
		return err
	}
	c.logger.Info("topic consumer started", zap.Object("topic", c.topic))
	return nil
}

// Stop shutdown and frees the resource held by this TopicConsumer and stops the batch DLQ producer.
func (c *TopicConsumer) Stop() {
	c.shutdown() // close each partition consumer
	c.dlq.Stop() // close DLQ, which also closes sarama AsyncProducer
	c.logger.Info("topic consumer stopped", zap.Object("topic", c.topic))
}

func (c *TopicConsumer) addPartitionConsumer(pc cluster.PartitionConsumer) {
	partition := pc.Partition()

	old, ok := c.partitionConsumerMap[partition]
	if ok {
		old.Stop()
		delete(c.partitionConsumerMap, partition)
	}
	c.logger.Info("topic consumer adding new partition consumer", zap.Int32("partition", partition))
	p := c.topic.PartitionConsumerFactory(c.topic, c.saramaConsumer, pc, c.options, c.msgC, c.dlq, c.scope, c.logger)
	c.partitionConsumerMap[partition] = p
	p.Start()
}

func (c *TopicConsumer) shutdown() {
	var wg sync.WaitGroup
	for _, pc := range c.partitionConsumerMap {
		wg.Add(1)
		go func(p PartitionConsumer) {
			p.Drain(2 * c.options.OffsetCommitInterval)
			wg.Done()
		}(pc)
	}
	wg.Wait()

	for k := range c.partitionConsumerMap {
		delete(c.partitionConsumerMap, k)
	}
}

// ResetOffset will reset the consumer offset for the specified topic, partition.
func (c *TopicConsumer) ResetOffset(partition int32, offsetRange kafka.OffsetRange) error {
	pc, ok := c.partitionConsumerMap[partition]
	if !ok {
		c.logger.Warn("failed to reset offset for non existent topic-partition", zap.Int32("partition", partition), zap.Object("offsetRange", offsetRange))
		return nil
	}

	return pc.ResetOffset(offsetRange)
}
