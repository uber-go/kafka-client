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

	cluster "github.com/bsm/sarama-cluster"
	"github.com/uber-go/kafka-client/internal/metrics"
	"github.com/uber-go/kafka-client/internal/util"
	"github.com/uber-go/kafka-client/kafka"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"time"
)

type (
	topicPartitionMap struct {
		topicPartitions map[TopicPartition]*partitionConsumer
	}

	// TopicPartition is a container for a (topic, partition) pair.
	TopicPartition struct {
		Topic     string
		Partition int32
	}

	partitionLimitMap struct {
		limits        map[TopicPartition]int64
		checkInterval time.Duration
		sleepInterval time.Duration
	}

	// clusterConsumer is an implementation of kafka consumer that consumes messages from a single cluster
	clusterConsumer struct {
		name       string
		topics     kafka.ConsumerTopicList
		cluster    string
		msgCh      chan kafka.Message
		consumer   SaramaConsumer
		producers  map[string]DLQ // Map of DLQ topic -> DLQ
		limits     partitionLimitMap
		partitions topicPartitionMap
		options    *Options
		tally      tally.Scope
		logger     *zap.Logger
		lifecycle  *util.RunLifecycle
		stopC      chan struct{}
		doneC      chan struct{}
	}
)

// NewClusterConsumer returns a Kafka Consumer that consumes from a single Kafka cluster
func NewClusterConsumer(
	groupName string,
	cluster string,
	options *Options,
	topics kafka.ConsumerTopicList,
	msgCh chan kafka.Message,
	consumer SaramaConsumer,
	producers map[string]DLQ,
	tally tally.Scope,
	logger *zap.Logger,
) (kafka.Consumer, error) {
	return newClusterConsumer(
		groupName,
		cluster,
		options,
		topics,
		msgCh,
		consumer,
		producers,
		options.Limits,
		tally,
		logger,
	)
}

func newClusterConsumer(
	groupName string,
	cluster string,
	options *Options,
	topics kafka.ConsumerTopicList,
	msgCh chan kafka.Message,
	consumer SaramaConsumer,
	producers map[string]DLQ,
	limits map[TopicPartition]int64,
	tally tally.Scope,
	logger *zap.Logger,
) (*clusterConsumer, error) {
	return &clusterConsumer{
		name:       groupName,
		topics:     topics,
		cluster:    cluster,
		msgCh:      msgCh,
		consumer:   consumer,
		producers:  producers,
		partitions: newPartitionMap(),
		limits: partitionLimitMap{
			limits:        limits,
			checkInterval: options.LimiterCheckInterval,
			sleepInterval: options.PartitionLimiterSleepTime,
		},
		options:   options,
		tally:     tally,
		logger:    logger,
		stopC:     make(chan struct{}),
		doneC:     make(chan struct{}),
		lifecycle: util.NewRunLifecycle(groupName+"-consumer-"+cluster, logger),
	}, nil
}

// Name returns the name of this consumer group
func (c *clusterConsumer) Name() string {
	return c.name
}

// Topics returns the topics that this consumer is subscribed to
func (c *clusterConsumer) Topics() []string {
	return c.topics.TopicNames()
}

// Start starts the consumer
func (c *clusterConsumer) Start() error {
	return c.lifecycle.Start(func() error {
		go c.eventLoop()
		c.tally.Counter(metrics.KafkaConsumerStarted).Inc(1)
		return nil
	})
}

// Stop stops the consumer
func (c *clusterConsumer) Stop() {
	c.lifecycle.Stop(func() {
		c.logger.Info("consumer shutting down")
		close(c.stopC)
		c.tally.Counter(metrics.KafkaConsumerStopped).Inc(1)
	})
}

// Closed returns a channel which will closed after this consumer is shutdown
func (c *clusterConsumer) Closed() <-chan struct{} {
	return c.doneC
}

// Messages returns the message channel for this consumer
func (c *clusterConsumer) Messages() <-chan kafka.Message {
	return c.msgCh
}

// eventLoop is the main event loop for this consumer
func (c *clusterConsumer) eventLoop() {
	limitCheckTicker := time.NewTicker(c.limits.checkInterval)
	c.logger.Info("consumer started")
	for {
		select {
		case <-limitCheckTicker.C:
			// limit checking occurs in the cluster consumer because we want to prevent partition rebalance for
			// a partitionConsumer that has reached its limit.
			reached, total := c.checkLimits()
			c.logger.With(
				zap.Int("reachedLimit", reached),
				zap.Int("total", total),
			).Debug("partition limits")
			if reached >= total {
				c.Stop()
			}
		case pc := <-c.consumer.Partitions():
			c.addPartition(pc)
		case n := <-c.consumer.Notifications():
			c.handleNotification(n)
		case err := <-c.consumer.Errors():
			c.logger.Error("consumer error", zap.Error(err))
		case <-c.stopC:
			c.shutdown()
			c.logger.Info("consumer stopped")
			return
		}
	}
}

func (c *clusterConsumer) checkLimits() (reached int, total int) {
	for _, pc := range c.partitions.topicPartitions {
		total++
		if pc.reachedLimit() {
			reached++
		}
	}
	return
}

// addPartition adds a new partition. If the partition already exist,
// it is first stopped before overwriting it with the new partition
func (c *clusterConsumer) addPartition(pc cluster.PartitionConsumer) {
	topic := pc.Topic()
	partition := pc.Partition()
	topicPartition := TopicPartition{Topic: topic, Partition: partition}

	old := c.partitions.Get(topicPartition)
	if old != nil {
		old.Stop()
		c.partitions.Delete(topicPartition)
	}
	c.logger.Info("new partition", zap.String("topic", topic), zap.Int32("partition", partition))

	// TODO (gteo): Consider using a blocking dlq implementation or throwing fatal error
	dlq, err := c.getDLQ(c.cluster, topic)
	if err != nil {
		c.logger.With(
			zap.Error(err),
			zap.String("topic", topic),
			zap.String("cluster", c.cluster),
		).Error("failed to find dlq producer for this topic so using noop dlq producer may result in data loss")
		dlq = NewDLQNoop()
	}

	pl := c.limits.Get(topicPartition)
	p := newPartitionConsumer(c.consumer, pc, pl, c.options, c.msgCh, dlq, c.tally, c.logger)
	c.partitions.Put(topicPartition, p)
	p.Start()
}

func (c *clusterConsumer) getDLQ(cluster, topic string) (DLQ, error) {
	consumerTopic, err := c.topics.GetConsumerTopicByClusterTopic(cluster, topic)
	if err != nil {
		return nil, err
	}

	dlq, ok := c.producers[consumerTopic.DLQ.HashKey()]
	if !ok {
		return nil, fmt.Errorf("no DLQ producer found")
	}

	return dlq, nil
}

// handleNotification is the handler that handles notifications
// from the underlying library about partition rebalances. There
// is no action taken in this handler except for logging.
func (c *clusterConsumer) handleNotification(n *cluster.Notification) {
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

// shutdown shutsdown the consumer
func (c *clusterConsumer) shutdown() {
	var wg sync.WaitGroup
	for _, pc := range c.partitions.topicPartitions {
		wg.Add(1)
		go func(p *partitionConsumer) {
			p.Drain(2 * c.options.OffsetCommitInterval)
			wg.Done()
		}(pc)
	}
	wg.Wait()
	c.partitions.Clear()
	c.consumer.Close()
	for _, p := range c.producers {
		p.Close()
	}
	close(c.doneC)
}

// newPartitionMap returns a partitionMap, a wrapper around a map
func newPartitionMap() topicPartitionMap {
	return topicPartitionMap{
		topicPartitions: make(map[TopicPartition]*partitionConsumer, 8),
	}
}

// Get returns the partition with the given id, if it exists
func (m *topicPartitionMap) Get(key TopicPartition) *partitionConsumer {
	p, ok := m.topicPartitions[key]
	if !ok {
		return nil
	}
	return p
}

// Delete deletes the partition with the given id
func (m *topicPartitionMap) Delete(key TopicPartition) {
	delete(m.topicPartitions, key)
}

// Put adds the partition with the given key
func (m *topicPartitionMap) Put(key TopicPartition, value *partitionConsumer) error {
	if m.Get(key) != nil {
		return fmt.Errorf("partition already exist")
	}
	m.topicPartitions[key] = value
	return nil
}

// Clear clears all entries in the map
func (m *topicPartitionMap) Clear() {
	for k := range m.topicPartitions {
		delete(m.topicPartitions, k)
	}
}

// Get returns a partitionLimiter with a limit set if the topicPartition exists in the limits map.
// If the limits map is not-empty but the key cannot be found, ti will return a partitionLimiter that always returns true
// so no messages will be consumed.
// If the limits map is nil, then it will return noopPartitionLimiter, which always allows all messages through.
func (m partitionLimitMap) Get(key TopicPartition) partitionLimiter {
	// If no limits, use passthrough partition limiter, which never blocks.
	if m.limits == nil {
		return newPassthroughPartitionLimiter(m.sleepInterval)
	}

	limit, ok := m.limits[key]
	// If limits exist but no limit exists for this partition, use blocking partition limiter, which
	// never allows any messages through.
	if !ok {
		return newBlockingPartitionLimiter(m.sleepInterval)
	}

	// If limits exist and there is a limit for this topic partition, use active partition limiter.
	return newActivePartitionLimiter(m.sleepInterval, limit)
}
