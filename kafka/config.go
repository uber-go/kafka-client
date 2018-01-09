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
	"fmt"
	"github.com/Shopify/sarama"
)

const (
	// OffsetOldest uses sequence number of oldest known message as the current offset
	OffsetOldest = sarama.OffsetOldest
	// OffsetNewest option uses sequence number of newest message as the current offset
	OffsetNewest = sarama.OffsetNewest
)

type (
	// Topics is an array of TopicConfig
	Topics []TopicConfig

	// TopicConfig contains the information about the topic and cluster to consume
	// and the DLQ topic and cluster for writing nack messages to.
	TopicConfig struct {
		// Topic is the name of the topic to consume from.
		Topic string
		// Cluster is the Cluster to consume this topic from.
		Cluster string
		// DLQTopic is a topic to write nack messages on.
		DLQTopic string
		// DLQCluster is a cluster to write nack messages to.
		DLQCluster string
	}

	// ConsumerConfig describes the config for a consumer group
	ConsumerConfig struct {
		// GroupName identifies your consumer group. Unless your application creates
		// multiple consumer groups (in which case it's suggested to have application name as
		// prefix of the group name), this should match your application name.
		GroupName string

		// Topics is a list of TopicConfig to consume from.
		Topics Topics

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
	}
)

// TopicsAsString returns a list of topics strings to consume
func (t Topics) TopicsAsString() []string {
	output := make([]string, 0, len(t))
	for _, topic := range t {
		output = append(output, topic.Topic)
	}
	return output
}

// FilterByCluster returns a new copy of Topics with TopicConfig matching the Cluster specified in params.
func (t Topics) FilterByCluster(cluster string) Topics {
	output := make([]TopicConfig, 0)
	for _, config := range t {
		if config.Cluster == cluster {
			output = append(output, config)
		}
	}
	return output
}

// GetByClusterAndTopic returns TopicConfig based on Cluster and Topic
func (t Topics) GetByClusterAndTopic(cluster, topic string) (TopicConfig, error) {
	for _, config := range t {
		if config.Cluster == cluster && config.Topic == topic {
			return config, nil
		}
	}
	return TopicConfig{}, fmt.Errorf("Unable to find TopicConfig with cluster=%s,topic=%s", cluster, topic)
}
