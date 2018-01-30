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
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
)

type (
	// Options are the tunable and injectable options for the consumer
	Options struct {
		RcvBufferSize          int // aggregate message buffer size
		PartitionRcvBufferSize int // message buffer size for each partition
		Concurrency            int // number of goroutines that will concurrently process messages
		OffsetPolicy           int64
		OffsetCommitInterval   time.Duration
		RebalanceDwellTime     time.Duration
		MaxProcessingTime      time.Duration // amount of time a partitioned consumer will wait during a drain
		ConsumerMode           cluster.ConsumerMode

		Limits TopicPartitionLimitMap
	}
)

// DefaultOptions returns the default options
func DefaultOptions() *Options {
	return &Options{
		Concurrency:            1024,
		RcvBufferSize:          2 * 1024, // twice the concurrency for compute/io overlap
		PartitionRcvBufferSize: 32,
		OffsetCommitInterval:   time.Second,
		RebalanceDwellTime:     time.Second,
		MaxProcessingTime:      250 * time.Millisecond,
		OffsetPolicy:           sarama.OffsetOldest,
		ConsumerMode:           cluster.ConsumerModePartitions,
		Limits:                 NewTopicPartitionLimitMap(nil),
	}
}
