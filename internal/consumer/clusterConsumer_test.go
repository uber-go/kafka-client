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
	"sync"
	"testing"
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
	ClusterConsumerTestSuite struct {
		suite.Suite
		consumer       *ClusterConsumer
		topicConsumer  *TopicConsumer
		saramaConsumer *mockSaramaConsumer
		dlqProducer    *mockDLQProducer
		msgCh          chan kafka.Message
		topic          string
		dlqTopic       string
		options        *Options
		logger         *zap.Logger
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
	}
}

func TestClusterConsumerTestSuite(t *testing.T) {
	suite.Run(t, new(ClusterConsumerTestSuite))
}

func (s *ClusterConsumerTestSuite) SetupTest() {
	topic := kafka.ConsumerTopic{
		Topic: kafka.Topic{
			Name:    "unit-test",
			Cluster: "production-cluster",
		},
		DLQ: kafka.Topic{
			Name:    "unit-test-dlq",
			Cluster: "dlq-cluster",
		},
	}
	s.topic = topic.Topic.Name
	s.dlqTopic = topic.DLQ.Name
	s.options = testConsumerOptions()
	s.logger = zap.NewNop()
	s.msgCh = make(chan kafka.Message, 10)
	s.saramaConsumer = newMockSaramaConsumer()
	s.dlqProducer = newMockDLQProducer()
	s.topicConsumer = NewTopicConsumer(Topic{ConsumerTopic: topic, DLQMetadataDecoder: NoopDLQMetadataDecoder, PartitionConsumerFactory: NewPartitionConsumer}, s.msgCh, s.saramaConsumer, s.dlqProducer, s.options, tally.NoopScope, s.logger)
	s.consumer = &ClusterConsumer{
		cluster:          topic.Cluster,
		consumer:         s.saramaConsumer,
		topicConsumerMap: map[string]*TopicConsumer{s.topic: s.topicConsumer},
		scope:            tally.NoopScope,
		logger:           s.logger,
		lifecycle:        util.NewRunLifecycle(topic.Cluster + "-consumer"),
		metricsTicker:    time.NewTicker(100 * time.Millisecond),
		stopC:            make(chan struct{}),
		doneC:            make(chan struct{}),
	}
}

func (s *ClusterConsumerTestSuite) TearDownTest() {
	s.consumer.Stop()
	<-s.consumer.Closed()
	s.True(util.AwaitCondition(func() bool { return s.saramaConsumer.isClosed() }, time.Second))
	s.True(s.dlqProducer.isClosed())
}

func (s *ClusterConsumerTestSuite) startWorker(count int, concurrency int, nack bool) *sync.WaitGroup {
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			for i := 0; i < count/concurrency; i++ {
				m := <-s.msgCh
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

func (s *ClusterConsumerTestSuite) TestWithOnePartition() {
	s.consumer.Start()
	workerWG := s.startWorker(100, 4, false)

	// send new partition to consumer
	p1 := newMockPartitionedConsumer(s.topic, 1, 0, s.options.PartitionRcvBufferSize)
	s.saramaConsumer.partitionC <- p1
	p1.start()

	// send notification about a rebalance
	n := &cluster.Notification{
		Claimed: map[string][]int32{s.topic: {1}},
		Current: map[string][]int32{s.topic: {1}},
	}
	s.saramaConsumer.notifyC <- n

	s.True(util.AwaitWaitGroup(workerWG, time.Second)) // wait for messages to be consumed

	// do assertions
	s.Equal(0, len(s.msgCh), "channel expected to be empty")
	s.True(util.AwaitCondition(func() bool { return s.saramaConsumer.offset(1) == int64(100) }, time.Second))
	s.Equal(0, s.dlqProducer.backlog())

	cp, ok := s.consumer.topicConsumerMap["unit-test"].partitionConsumerMap[1].(*partitionConsumer)
	s.True(ok)
	s.Equal(int64(100), s.saramaConsumer.offset(1), "wrong commit offset")
	s.True(cp.ackMgr.unackedSeqList.list.Empty(), "unacked offset list must be empty")

	// test shutdown
	p1.stop()
	s.True(util.AwaitCondition(func() bool { return p1.isClosed() }, time.Second))
}

func (s *ClusterConsumerTestSuite) TestWithManyPartitions() {
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
	s.Equal(0, len(s.msgCh))
	for i := 0; i < nPartitions; i++ {
		s.True(util.AwaitCondition(func() bool { return s.saramaConsumer.offset(i) == int64(100) }, time.Second))
		cp, ok := s.consumer.topicConsumerMap["unit-test"].partitionConsumerMap[1].(*partitionConsumer)
		s.True(ok)
		s.Equal(int64(100), s.saramaConsumer.offset(i), "wrong commit offset")
		s.True(cp.ackMgr.unackedSeqList.list.Empty(), "unacked offset list must be empty")
	}
	s.Equal(0, s.dlqProducer.backlog())
}

func (s *ClusterConsumerTestSuite) TestPartitionRebalance() {
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
		s.Equal(0, len(s.msgCh))
		for i := 0; i < nPartitions; i++ {
			off := int64(100 * (r + 1))
			s.True(util.AwaitCondition(func() bool { return s.saramaConsumer.offset(i) == off }, time.Minute))
			s.Equal(off, s.saramaConsumer.offset(i), "wrong commit offset for partition %v", i)
		}
	}
	s.Equal(0, s.dlqProducer.backlog())
}

func (s *ClusterConsumerTestSuite) TestDuplicates() {
	s.consumer.Start()
	workerWG := s.startWorker(100, 1, false)
	pc := newMockPartitionedConsumer(s.topic, 1, 0, s.options.PartitionRcvBufferSize)
	s.saramaConsumer.partitionC <- pc
	// start two parallel message producers for the same offsets
	pc.start()
	pc.start()
	s.True(util.AwaitWaitGroup(workerWG, 2*time.Second)) // wait for all messages to be consumed
	s.Equal(0, len(s.msgCh))
	s.True(util.AwaitCondition(func() bool { return s.saramaConsumer.offset(1) == int64(100) }, time.Second))
	s.Equal(int64(100), s.saramaConsumer.offset(1))
	s.Equal(0, s.dlqProducer.backlog())
}

func (s *ClusterConsumerTestSuite) TestDLQ() {
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
	s.Equal(0, len(s.msgCh))
	for i := 0; i < nPartitions; i++ {
		s.True(util.AwaitCondition(func() bool { return s.saramaConsumer.offset(i) == int64(100) }, time.Second))
		cp, ok := s.consumer.topicConsumerMap["unit-test"].partitionConsumerMap[int32(i)].(*partitionConsumer)
		s.True(ok)
		s.Equal(int64(100), s.saramaConsumer.offset(i), "wrong commit offset")
		s.True(cp.ackMgr.unackedSeqList.list.Empty(), "unacked offset list must be empty")
	}
	s.Equal(nPartitions*100, s.dlqProducer.backlog())
}
