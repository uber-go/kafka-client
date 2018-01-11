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
)

type (
	// multiConsumerMap is a map that contains multiple kafka consumers
	multiConsumerMap struct {
		name              string
		topics            kafka.ConsumerTopicList
		consumers         map[string]kafka.Consumer
		saramaConnections *SaramaClusters
		msgCh             chan kafka.Message
		doneC             chan struct{}
		tally             tally.Scope
		logger            *zap.Logger
		lifecycle         *util.RunLifecycle
	}

	topicPartitionMap struct {
		topicPartitions map[topicPartition]*partitionConsumer
	}

	topicPartition struct {
		topic     string
		partition int32
	}

	// clusterConsumer is an implementation of kafka consumer that consumes messages from a single cluster
	clusterConsumer struct {
		name       string
		topics     kafka.ConsumerTopicList
		cluster    string
		msgCh      chan kafka.Message
		consumer   SaramaConsumer
		producers  map[kafka.Topic]DLQ // Map of DLQ topic -> DLQ
		partitions topicPartitionMap
		options    *Options
		tally      tally.Scope
		logger     *zap.Logger
		lifecycle  *util.RunLifecycle
		stopC      chan struct{}
		doneC      chan struct{}
	}
)

// New returns a new kafka consumer for a given topic
// the returned consumer can be used to consume and process
// messages across multiple go routines. The number of routines
// that will process messages in parallel MUST be pre-configured
// through the ConsumerConfig. And after each message is processed,
// either msg.Ack or msg.Nack must be called to advance the offsets
//
// During failures / partition rebalances, this consumer does a
// best effort at avoiding duplicates, but the application must be
// designed for idempotency
func New(
	config *kafka.ConsumerConfig,
	consumers map[string]kafka.Consumer,
	sarama *SaramaClusters,
	msgCh chan kafka.Message,
	scope tally.Scope,
	log *zap.Logger) (kafka.Consumer, error) {
	return newMultiConsumerMap(config, consumers, sarama, msgCh, scope, log)
}

func newMultiConsumerMap(
	config *kafka.ConsumerConfig,
	consumers map[string]kafka.Consumer,
	sarama *SaramaClusters,
	msgCh chan kafka.Message,
	scope tally.Scope,
	log *zap.Logger,
) (*multiConsumerMap, error) {
	return &multiConsumerMap{
		name:              config.GroupName,
		topics:            config.TopicList,
		consumers:         consumers,
		saramaConnections: sarama,
		msgCh:             msgCh,
		tally:             scope,
		logger:            log,
		lifecycle:         util.NewRunLifecycle(config.GroupName+"-consumer", log),
	}, nil
}

// NewClusterConsumer returns a Kafka Consumer that consumes from a single Kafka cluster
func NewClusterConsumer(
	cluster string,
	config *kafka.ConsumerConfig,
	options *Options,
	topics kafka.ConsumerTopicList,
	msgCh chan kafka.Message,
	consumer SaramaConsumer,
	producers map[kafka.Topic]DLQ,
	tally tally.Scope,
	logger *zap.Logger,
) (kafka.Consumer, error) {
	return newClusterConsumer(
		cluster,
		config,
		options,
		topics,
		msgCh,
		consumer,
		producers,
		tally,
		logger,
	)
}

func newClusterConsumer(
	cluster string,
	config *kafka.ConsumerConfig,
	options *Options,
	topics kafka.ConsumerTopicList,
	msgCh chan kafka.Message,
	consumer SaramaConsumer,
	producers map[kafka.Topic]DLQ,
	tally tally.Scope,
	logger *zap.Logger,
) (*clusterConsumer, error) {
	return &clusterConsumer{
		name:       config.GroupName,
		topics:     topics,
		cluster:    cluster,
		msgCh:      msgCh,
		consumer:   consumer,
		producers:  producers,
		partitions: newPartitionMap(),
		options:    options,
		tally:      tally,
		logger:     logger,
		stopC:      make(chan struct{}),
		doneC:      make(chan struct{}),
		lifecycle:  util.NewRunLifecycle(config.GroupName+"-consumer-"+cluster, logger),
	}, nil
}

func (c *multiConsumerMap) Name() string {
	return c.name
}

func (c *multiConsumerMap) Topics() []string {
	return c.topics.TopicNames()
}

func (c *multiConsumerMap) Start() error {
	return c.lifecycle.Start(func() error {
		errAcc := newErrorAccumulator()
		for clusterName, consumer := range c.consumers {
			if err := consumer.Start(); err != nil {
				c.logger.With(zap.Error(err)).Error(fmt.Sprintf("Failed to start consumer for cluster=%s", clusterName))
				errAcc = append(errAcc, err)
				continue
			}
		}
		return errAcc.ToError()
	})
}

func (c *multiConsumerMap) Stop() {
	c.lifecycle.Stop(func() {
		for _, consumer := range c.consumers {
			consumer.Stop()
		}

		c.saramaConnections.Lock()
		for _, clusterConnection := range c.saramaConnections.Clusters {
			clusterConnection.Lock()
			if clusterConnection.Consumer != nil {
				clusterConnection.Consumer.Close()
			}
			if clusterConnection.Producer != nil {
				clusterConnection.Producer.Close()
			}
			clusterConnection.Unlock()
		}
		c.saramaConnections.Unlock()
		close(c.doneC)
	})
}

func (c *multiConsumerMap) Closed() <-chan struct{} {
	return c.doneC
}

func (c *multiConsumerMap) Messages() <-chan kafka.Message {
	return c.msgCh
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

// Closed returns a channel which will closed after this consumer is shutown
func (c *clusterConsumer) Closed() <-chan struct{} {
	return c.doneC
}

// Messages returns the message channel for this consumer
func (c *clusterConsumer) Messages() <-chan kafka.Message {
	return c.msgCh
}

// eventLoop is the main event loop for this consumer
func (c *clusterConsumer) eventLoop() {
	c.logger.Info("consumer started")
	for {
		select {
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

// addPartition adds a new partition. If the partition already exist,
// it is first stopped before overwriting it with the new partition
func (c *clusterConsumer) addPartition(pc cluster.PartitionConsumer) {
	old := c.partitions.Get(topicPartition{pc.Topic(), pc.Partition()})
	if old != nil {
		old.Stop()
		c.partitions.Delete(topicPartition{pc.Topic(), pc.Partition()})
	}
	c.logger.Info("new partition", zap.String("topic", pc.Topic()), zap.Int32("partition", pc.Partition()))

	// TODO (gteo): Consider using a blocking dlq implementation or throwing fatal error
	dlq, err := c.getDLQ(pc.Topic(), c.cluster)
	if err != nil {
		c.logger.With(
			zap.Error(err),
			zap.String("topic", pc.Topic()),
			zap.String("cluster", c.cluster),
		).Error("failed to find dlq producer for this topic so using noop dlq producer may result in data loss")
		dlq = NewDLQNoop()
	}

	p := newPartitionConsumer(c.consumer, pc, c.options, c.msgCh, dlq, c.tally, c.logger)
	c.partitions.Put(topicPartition{pc.Topic(), pc.Partition()}, p)
	p.Start()
}

func (c *clusterConsumer) getDLQ(topic, cluster string) (DLQ, error) {
	consumerTopic, err := c.topics.FilterByClusterTopic(cluster, topic)
	if err != nil {
		return nil, err
	}

	dlq, ok := c.producers[consumerTopic.DLQ]
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
	c.consumer.CommitOffsets()
	c.consumer.Close()
	close(c.doneC)
}

// newPartitionMap returns a partitionMap, a wrapper around a map
func newPartitionMap() topicPartitionMap {
	return topicPartitionMap{
		topicPartitions: make(map[topicPartition]*partitionConsumer, 8),
	}
}

// Get returns the partition with the given id, if it exists
func (m *topicPartitionMap) Get(key topicPartition) *partitionConsumer {
	p, ok := m.topicPartitions[key]
	if !ok {
		return nil
	}
	return p
}

// Delete deletes the partition with the given id
func (m *topicPartitionMap) Delete(key topicPartition) {
	delete(m.topicPartitions, key)
}

// Put adds the partition with the given key
func (m *topicPartitionMap) Put(key topicPartition, value *partitionConsumer) error {
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
