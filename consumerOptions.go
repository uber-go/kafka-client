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
	"github.com/uber-go/kafka-client/internal/consumer"
)

// ConsumerOption is the type for optional arguments to the NewConsumer constructor.
type (
	ConsumerOption interface {
		apply(*consumer.Options)
	}
	consumerLimitOption struct {
		limits map[consumer.TopicPartition]int64
	}
)

// WithConsumerLimits sets consumer limits for a consumer.
// If consumer limits are set, the consumer will only consume messages from the specified topic-partitions
// up to the limits then shut itself down.
// If limits are set and the consumer is assigned a topic-partition that is not in the limits map, no messages
// will be received for that topic partition.
func WithConsumerLimits(limits map[string]map[int32]int64) ConsumerOption {
	topicPartitionLimits := make(map[consumer.TopicPartition]int64)
	for topic, partitionMap := range limits {
		for partition, offset := range partitionMap {
			topicPartitionLimits[consumer.TopicPartition{Topic: topic, Partition: partition}] = offset
		}
	}
	return &consumerLimitOption{
		limits: topicPartitionLimits,
	}
}

func (c consumerLimitOption) apply(opts *consumer.Options) {
	opts.Limits = consumer.NewTopicPartitionLimitMap(c.limits)
}
