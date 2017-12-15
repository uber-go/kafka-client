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

package kafka

import (
	"github.com/Shopify/sarama"
)

const (
	// OffsetOldest uses sequence number of oldest known message as the current offset
	OffsetOldest = sarama.OffsetOldest
	// OffsetNewest option uses sequence number of newest message as the current offset
	OffsetNewest = sarama.OffsetNewest
)

type (
	// DLQConfig contains the configuration for consumer Dead Letter Queue
	DLQConfig struct {
		// Name of the dlq topic. If empty, dlq will be disabled for this consumer
		Name string
		// Name of the cluster hosting this DLQ
		Cluster string
	}
	// ConsumerConfig describes the config for a consumer group
	ConsumerConfig struct {
		// GroupName identifies your consumer group. Unless your application creates
		// multiple consumer groups (in which case it's suggested to have application name as
		// prefix of the group name), this should match your application name.
		GroupName string

		// Topic is the name of topic to consume from.
		Topic string

		// Cluster is the name of the cluster hosting this topic
		Cluster string

		// OffsetConfig is the offset-handling policy for this consumer group.
		Offsets struct {
			// Initial specifies the fallback offset configuration on consumer start.
			// The consumer will use the offsets persisted from its last run unless \
			// the offsets are too old or too new.
			Initial struct {
				// Offset is the initial offset to use if there is no previous offset
				// committed. Use OffsetNewest for high watermark and OffsetOldest for
				// low watermark. Defaults to OffsetOldest.
				Offset int64
				// Reset should be set to true if you wish for the consumer to overwrite
				// an existing checkpoint with the offset specified in `Offset`.
				// The overwritten offset cannot be recovered.
				Reset bool
			}
		}

		// Concurrency determines the number of concurrent messages to process.
		// When using the handler based API, this corresponds to the number of concurrent go
		// routines handler functions the library will run. Default is 1.
		Concurrency int

		// DLQ defines the configuration for Dead Letter Queue
		DLQ DLQConfig
	}
)
