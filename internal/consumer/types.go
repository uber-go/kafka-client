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
	"github.com/uber-go/kafka-client/internal/util"
	"go.uber.org/zap"
)

type (
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
		// Limits are the consumer limits per topic partitions
		Limits map[TopicPartition]int64
	}

	// saramaConsumer is an internal version of SaramaConsumer that implements a close method that can be safely called
	// multiple times.
	saramaConsumer struct {
		SaramaConsumer
		lifecycle *util.RunLifecycle
	}

	// saramaProducer is an internal version of SaramaConsumer that implements a close method that can be safely called
	// multiple times.
	saramaProducer struct {
		sarama.SyncProducer
		lifecycle *util.RunLifecycle
	}
)

// NewSaramaConsumer returns a new SaramaConsumer that has a Close method that can be called multiple times.
func NewSaramaConsumer(brokers []string, groupID string, topics []string, config *cluster.Config) (SaramaConsumer, error) {
	c, err := cluster.NewConsumer(brokers, groupID, topics, config)
	if err != nil {
		return nil, err
	}

	return newSaramaConsumer(c)
}

func newSaramaConsumer(c SaramaConsumer) (SaramaConsumer, error) {
	sc := saramaConsumer{
		SaramaConsumer: c,
		lifecycle:      util.NewRunLifecycle("sarama-consumer", zap.NewNop()),
	}

	sc.lifecycle.Start(func() error { return nil }) // must start lifecycle so stop will stop

	return &sc, nil
}

// NewSaramaProducer returns a new SyncProducer that has Close method that can be called multiple times.
func NewSaramaProducer(brokers []string, config *sarama.Config) (sarama.SyncProducer, error) {
	p, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}

	return newSaramaProducer(p)
}

func newSaramaProducer(p sarama.SyncProducer) (sarama.SyncProducer, error) {
	sp := saramaProducer{
		SyncProducer: p,
		lifecycle:    util.NewRunLifecycle("sarama-producer", zap.NewNop()),
	}

	sp.lifecycle.Start(func() error { return nil }) // must start lifecycle so stop will stop

	return &sp, nil
}

// Close overwrites the underlying sarama SyncProducer Close method with one that can be safely called multiple times.
//
// This close will always return nil error.
func (p *saramaProducer) Close() error {
	p.lifecycle.Stop(func() {
		p.SyncProducer.Close()
	})
	return nil
}

// Close overwrites the underlying SaramaConsumer Close method with one that can be safely called multiple times.
//
// This close will always return nil error.
func (p *saramaConsumer) Close() error {
	p.lifecycle.Stop(func() {
		p.SaramaConsumer.Close()
	})
	return nil
}
