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

import "time"

const (
	noLimit = -2
	// defaultLimit set to -1 so that if you use default limit, no messages will be processed.
	defaultLimit              = -1
	defaultLimitCheckInterval = time.Second
)

// TopicPartitionLimitMap is a map of limits for topic partitions
type TopicPartitionLimitMap struct {
	// checkInterval determines the frequency the clusterConsumer will check that all partitionConsumers
	// have consumed up to their individual limits.
	checkInterval time.Duration
	// limits determine the highest offset a consumer with limits enabled will consume up to.
	// If the limits map is empty, limits will be effectively infinite and all messages will be received.
	// If there is any TopicPartition set in the limits map, then TopicPartitions will only be consumed
	// up to the specified limit offset.
	// If limits are set (i.e., map not empty), but the limit for a particular TopicPartition is not found,
	// then a limit of -1 will be used so no messages from that TopicPartition will be consumed.
	limits map[TopicPartition]int64
}

// NewTopicPartitionLimitMap returns a topic partition limit map with limits set.
func NewTopicPartitionLimitMap(limit map[TopicPartition]int64) TopicPartitionLimitMap {
	return TopicPartitionLimitMap{
		limits:        limit,
		checkInterval: defaultLimitCheckInterval,
	}
}

// HasLimits returns true if there are limits set.
func (m *TopicPartitionLimitMap) HasLimits() bool {
	return m.limits != nil
}

// Get returns noLimit if there are no limits set.
// If there are limits set but no limits for this topic partition, then defaultLimit will be used.
// Else, it will return the limit stored in the limits map.
func (m *TopicPartitionLimitMap) Get(tp TopicPartition) int64 {
	if m.limits == nil {
		return noLimit
	}

	limit, ok := m.limits[tp]
	if !ok {
		return defaultLimit
	}

	return limit
}
