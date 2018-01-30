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
	"testing"

	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/kafka-client/internal/util"
	"github.com/uber-go/kafka-client/kafka"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type (
	ConsumerTestSuite struct {
		suite.Suite
		consumer       *clusterConsumer
		saramaConsumer *mockSaramaConsumer
		dlqProducer    *mockDLQProducer
		topic          string
		dlqTopic       string
		options        *Options
		logger         *zap.Logger
		limits         TopicPartitionLimitMap
	}
)

func testConsumerOptions() *Options {
	return &Options{
		Concurrency:            4,
		RcvBufferSize:          4,
		PartitionRcvBufferSize: 2,
		OffsetCommitInterval:   25 * time.Millisecond,
		RebalanceDwellTime:     time.Second,
		MaxProcessingTime:      5 * time.Millisecond,
		OffsetPolicy:           sarama.OffsetOldest,
		Limits:                 NewTopicPartitionLimitMap(nil),
	}
}

var _ kafka.Consumer = (*clusterConsumer)(nil)

func TestConsumerTestSuite(t *testing.T) {
	suite.Run(t, new(ConsumerTestSuite))
}

func (s *ConsumerTestSuite) SetupTest() {
	topic := kafka.ConsumerTopic{
		Topic: kafka.Topic{
			Name:       "unit-test",
			Cluster:    "production-cluster",
			BrokerList: nil,
		},
		DLQ: kafka.Topic{
			Name:       "unit-test-dlq",
			Cluster:    "dlq-cluster",
			BrokerList: nil,
		},
	}
	s.topic = topic.Topic.Name
	s.dlqTopic = topic.DLQ.Name
	config := &kafka.ConsumerConfig{
		TopicList:   []kafka.ConsumerTopic{topic},
		GroupName:   "unit-test-cg",
		Concurrency: 4,
	}
	s.options = testConsumerOptions()
	s.limits = NewTopicPartitionLimitMap(nil)
	s.logger = zap.NewNop()
	s.dlqProducer = newMockDLQProducer()
	s.saramaConsumer = newMockSaramaConsumer()
	msgCh := make(chan kafka.Message)
	var err error
	dlq := map[string]DLQ{
		topic.DLQ.HashKey(): NewDLQ(s.dlqTopic, topic.DLQ.Cluster, s.dlqProducer, tally.NoopScope, s.logger),
	}
	s.consumer, err = newClusterConsumer(config.GroupName, topic.Cluster, s.options, config.TopicList, msgCh, s.saramaConsumer, dlq, s.limits, tally.NoopScope, s.logger)
	s.NoError(err)
}

func (s *ConsumerTestSuite) TearDownTest() {
	s.consumer.Stop()
	<-s.consumer.Closed()
	s.True(util.AwaitCondition(func() bool { return s.saramaConsumer.isClosed() }, time.Second))
	s.True(s.dlqProducer.isClosed())
}

func (s *ConsumerTestSuite) startWorker(count int, concurrency int, nack bool) *sync.WaitGroup {
	var wg sync.WaitGroup
	msgCh := s.consumer.Messages()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < count/concurrency; i++ {
				m := <-msgCh
				if nack {
					m.Nack()
					continue
				}
				m.Ack()
			}
			wg.Done()
		}()
	}
	return &wg
}

func (s *ConsumerTestSuite) TestWithOnePartition() {
	s.consumer.Start()
	workerWG := s.startWorker(100, 4, false)

	// send new partition to consumer
	p1 := newMockPartitionedConsumer(s.topic, 1, 0, s.options.PartitionRcvBufferSize)
	s.saramaConsumer.partitionC <- p1
	p1.start()

	// send notification about a rebalance
	n := &cluster.Notification{Claimed: make(map[string][]int32)}
	n.Claimed[s.topic] = []int32{1}
	s.saramaConsumer.notifyC <- n

	s.True(util.AwaitWaitGroup(workerWG, time.Second)) // wait for messages to be consumed

	// do assertions
	s.Equal(0, len(s.consumer.msgCh), "channel expected to be empty")
	s.True(util.AwaitCondition(func() bool { return s.saramaConsumer.offset(1) == int64(100) }, time.Second))
	s.Equal(0, s.dlqProducer.backlog())

	cp := s.consumer.partitions.Get(TopicPartition{"unit-test", 1})
	s.Equal(int64(100), s.saramaConsumer.offset(1), "wrong commit offset")
	s.True(cp.ackMgr.unackedSeqList.list.Empty(), "unacked offset list must be empty")

	// test shutdown
	p1.stop()
	s.True(util.AwaitCondition(func() bool { return p1.isClosed() }, time.Second))
}

func (s *ConsumerTestSuite) TestWithManyPartitions() {
	nPartitions := 8
	s.consumer.Start()
	workerWG := s.startWorker(nPartitions*100, 4, false)
	// start all N partitions
	for i := 0; i < nPartitions; i++ {
		pc := newMockPartitionedConsumer(s.topic, int32(i), 0, s.options.PartitionRcvBufferSize)
		s.saramaConsumer.partitionC <- pc
		pc.start()
		if i%2 == 0 {
			// send notification about a rebalance
			n := &cluster.Notification{Claimed: make(map[string][]int32)}
			n.Claimed[s.topic] = []int32{int32(i), int32(i - 1)}
			s.saramaConsumer.notifyC <- n
		}
	}
	s.True(util.AwaitWaitGroup(workerWG, 2*time.Second)) // wait for all messages to be consumed
	s.Equal(0, len(s.consumer.msgCh))
	for i := 0; i < nPartitions; i++ {
		s.True(util.AwaitCondition(func() bool { return s.saramaConsumer.offset(i) == int64(100) }, time.Second))
		cp := s.consumer.partitions.Get(TopicPartition{"unit-test", 1})
		s.Equal(int64(100), s.saramaConsumer.offset(i), "wrong commit offset")
		s.True(cp.ackMgr.unackedSeqList.list.Empty(), "unacked offset list must be empty")
	}
	s.Equal(0, s.dlqProducer.backlog())
}

func (s *ConsumerTestSuite) TestPartitionRebalance() {
	nPartitions := 4
	nRebalances := 3
	s.consumer.Start()
	for r := 0; r < nRebalances; r++ {
		workerWG := s.startWorker(nPartitions*100, 4, false)
		partitions := make([]*mockPartitionedConsumer, nPartitions)
		// start all N partitions
		for i := 0; i < nPartitions; i++ {
			pc := newMockPartitionedConsumer(s.topic, int32(i), int64(r*100), s.options.PartitionRcvBufferSize)
			partitions[i] = pc
			s.saramaConsumer.partitionC <- pc
			pc.start()
		}
		s.True(util.AwaitWaitGroup(workerWG, 2*time.Second)) // wait for all messages to be consumed
		s.Equal(0, len(s.consumer.msgCh))
		for i := 0; i < nPartitions; i++ {
			off := int64(100 * (r + 1))
			s.True(util.AwaitCondition(func() bool { return s.saramaConsumer.offset(i) == off }, time.Minute))
			s.Equal(off, s.saramaConsumer.offset(i), "wrong commit offset for partition %v", i)
		}
	}
	s.Equal(0, s.dlqProducer.backlog())
}

func (s *ConsumerTestSuite) TestDuplicates() {
	s.consumer.Start()
	workerWG := s.startWorker(100, 1, false)
	pc := newMockPartitionedConsumer(s.topic, 1, 0, s.options.PartitionRcvBufferSize)
	s.saramaConsumer.partitionC <- pc
	// start two parallel message producers for the same offsets
	pc.start()
	pc.start()
	s.True(util.AwaitWaitGroup(workerWG, 2*time.Second)) // wait for all messages to be consumed
	s.Equal(0, len(s.consumer.msgCh))
	s.True(util.AwaitCondition(func() bool { return s.saramaConsumer.offset(1) == int64(100) }, time.Second))
	s.Equal(int64(100), s.saramaConsumer.offset(1))
	s.Equal(0, s.dlqProducer.backlog())
}

func (s *ConsumerTestSuite) TestDLQ() {
	nPartitions := 4
	s.consumer.Start()
	workerWG := s.startWorker(nPartitions*100, 4, true)
	// start all N partitions
	for i := 0; i < nPartitions; i++ {
		pc := newMockPartitionedConsumer(s.topic, int32(i), 0, s.options.PartitionRcvBufferSize)
		s.saramaConsumer.partitionC <- pc
		pc.start()
	}
	s.True(util.AwaitWaitGroup(workerWG, 2*time.Second)) // wait for all messages to be consumed
	s.Equal(0, len(s.consumer.msgCh))
	for i := 0; i < nPartitions; i++ {
		s.True(util.AwaitCondition(func() bool { return s.saramaConsumer.offset(i) == int64(100) }, time.Second))
		cp := s.consumer.partitions.Get(TopicPartition{"unit-test", int32(i)})
		s.Equal(int64(100), s.saramaConsumer.offset(i), "wrong commit offset")
		s.True(cp.ackMgr.unackedSeqList.list.Empty(), "unacked offset list must be empty")
	}
	s.Equal(nPartitions*100, s.dlqProducer.backlog())
}

func (s *ConsumerTestSuite) TestConsumerWithLimit() {
	// partition 0 will receive 1 message
	// partition 1 will receive 0 messages
	limits := NewTopicPartitionLimitMap(map[TopicPartition]int64{
		{Topic: s.topic, Partition: 0}: 1,
	})
	limits.checkInterval = time.Millisecond
	s.consumer.limits = limits

	s.NoError(s.consumer.Start())
	s.startWorker(100, 1, false)

	// Create 2 partitions consumers, each sending 100 messages
	// Only 1 of these messages should pass though limit
	nPartitions := 2
	for i := 0; i < nPartitions; i++ {
		pc := newMockPartitionedConsumer(s.topic, int32(i), 0, s.options.PartitionRcvBufferSize)
		s.saramaConsumer.partitionC <- pc
		pc.start()
	}

	// Sleep some amount of time to ensure messages get processed
	time.Sleep(100 * time.Millisecond)
	s.Equal(0, len(s.consumer.msgCh))

	// Consumer should auto close once limit has been reached
	util.AwaitCondition(func() bool {
		<-s.consumer.Closed()
		return true
	}, 100*time.Millisecond)
}
