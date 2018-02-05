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
		topic                string
		msgC                 chan kafka.Message
		partitionConsumerMap map[int32]*partitionConsumer
		dlq                  DLQ
		saramaConsumer       SaramaConsumer // SaramaConsumer is a shared resource that is owned by clusterConsumer.
		options              *Options
		scope                tally.Scope
		logger               *zap.Logger
	}
)

// NewTopicConsumer returns a new TopicConsumer for consuming from a single topic.
func NewTopicConsumer(
	topic string,
	msgC chan kafka.Message,
	dlq DLQ,
	options *Options,
	scope tally.Scope,
	logger *zap.Logger,
) *TopicConsumer {
	return &TopicConsumer{
		topic:                topic,
		msgC:                 msgC,
		partitionConsumerMap: make(map[int32]*partitionConsumer),
		dlq:                  dlq,
		saramaConsumer:       nil,
		options:              options,
		scope:                scope,
		logger:               logger,
	}
}

// Topic returns the topic that this TopicConsumer is responsible for.
func (c *TopicConsumer) Topic() string {
	return c.topic
}

// SetSaramaConsumer is a sets the SaramaConsumer for this TopicConsumer.
func (c *TopicConsumer) SetSaramaConsumer(sc SaramaConsumer) *TopicConsumer {
	c.saramaConsumer = sc
	return c
}

// Start the DLQ consumer goroutine.
func (c *TopicConsumer) Start() error {
	return c.dlq.Start()
}

// Stop shutdown and frees the resource held by this TopicConsumer and stops the batch DLQ producer.
func (c *TopicConsumer) Stop() {
	c.shutdown() // close each partition consumer
	c.dlq.Stop() // close DLQ, which also closes sarama AsyncProducer
}

func (c *TopicConsumer) addPartitionConsumer(pc cluster.PartitionConsumer) {
	partition := pc.Partition()

	old, ok := c.partitionConsumerMap[partition]
	if ok {
		old.Stop()
		delete(c.partitionConsumerMap, partition)
	}
	c.logger.Info("new partition consumer", zap.Int32("partition", partition))
	// TODO (gteo): fix limiter
	p := newPartitionConsumer(c.saramaConsumer, pc, noLimit, c.options, c.msgC, c.dlq, c.scope, c.logger)
	c.partitionConsumerMap[partition] = p
	p.Start()
}

func (c *TopicConsumer) shutdown() {
	var wg sync.WaitGroup
	for _, pc := range c.partitionConsumerMap {
		wg.Add(1)
		go func(p *partitionConsumer) {
			p.Drain(2 * c.options.OffsetCommitInterval)
			wg.Done()
		}(pc)
	}
	wg.Wait()

	for k := range c.partitionConsumerMap {
		delete(c.partitionConsumerMap, k)
	}
}
