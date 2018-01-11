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
	"sync"
)

type (
	// SaramaCluster is a cluster specific threadsafe container for Sarama consumers and producers.
	// If you access the Consumer/Producer type directly, you are responsible for acquiring the RWLock.
	SaramaCluster struct {
		sync.RWMutex
		Consumer SaramaConsumer
		Producer sarama.SyncProducer
	}

	// SaramaClusters is a thredsafe map of SaramaClusters.
	// If you access the Consumer/Producer type directly, you are responsible for acquiring the RWLock.
	SaramaClusters struct {
		sync.RWMutex
		Clusters map[string]*SaramaCluster
	}

	// SaramaConsumer is an interface for external consumer library (sarama)
	SaramaConsumer interface {
		Close() error
		Errors() <-chan error
		Notifications() <-chan *cluster.Notification
		Partitions() <-chan cluster.PartitionConsumer
		CommitOffsets() error
		Messages() <-chan *sarama.ConsumerMessage
		HighWaterMarks() map[string]map[int32]int64
		MarkOffset(msg *sarama.ConsumerMessage, metadata string)
		MarkPartitionOffset(topic string, partition int32, offset int64, metadata string)
	}
	// Options contains the tunables for a
	// kafka consumer. Currently, these options are
	// not configurable except for unit test
	Options struct {
		RcvBufferSize          int // aggregate message buffer size
		PartitionRcvBufferSize int // message buffer size for each partition
		Concurrency            int // number of goroutines that will concurrently process messages
		OffsetPolicy           int64
		OffsetCommitInterval   time.Duration
		RebalanceDwellTime     time.Duration
		MaxProcessingTime      time.Duration // amount of time a partitioned consumer will wait during a drain
		ConsumerMode           cluster.ConsumerMode
	}
)
