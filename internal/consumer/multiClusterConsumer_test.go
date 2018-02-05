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
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/kafka-client/kafka"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type MultiClusterConsumerTestSuite struct {
	suite.Suite
	consumer *MultiClusterConsumer
	config   *kafka.ConsumerConfig
	topics   kafka.ConsumerTopicList
	options  *Options
	msgCh    chan kafka.Message
}

// verify *MultiClusterConsumer is implements kafka.Consumer
var _ kafka.Consumer = (*MultiClusterConsumer)(nil)

func (s *MultiClusterConsumerTestSuite) SetupTest() {
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
	s.topics = []kafka.ConsumerTopic{topic}
	s.config = &kafka.ConsumerConfig{
		TopicList:   s.topics,
		GroupName:   "unit-test-cg",
		Concurrency: 4,
	}
	s.options = testConsumerOptions()
	s.msgCh = make(chan kafka.Message)
	s.consumer = NewMultiClusterConsumer(
		s.config.GroupName,
		s.topics,
		make(map[string]*ClusterConsumer),
		make(map[string]sarama.Client),
		s.msgCh,
		tally.NoopScope,
		zap.L(),
	)
	s.Equal(s.config.GroupName, s.consumer.Name())
	s.Equal(s.config.TopicList.TopicNames(), s.consumer.Topics())
}

func (s *MultiClusterConsumerTestSuite) TeardownTest() {
	s.consumer.Stop()
}

func TestMultiClusterConsumerSuite(t *testing.T) {
	suite.Run(t, new(MultiClusterConsumerTestSuite))
}

func (s *MultiClusterConsumerTestSuite) TestStartSucceeds() {
	cc1 := NewClusterConsumer("cc1", newMockSaramaConsumer(), make(map[string]*TopicConsumer), tally.NoopScope, zap.NewNop())
	cc2 := NewClusterConsumer("cc2", newMockSaramaConsumer(), make(map[string]*TopicConsumer), tally.NoopScope, zap.NewNop())
	s.consumer.clusterConsumerMap["cc1"] = cc1
	s.consumer.clusterConsumerMap["cc2"] = cc2

	s.NoError(s.consumer.Start())

	started, stopped := cc1.lifecycle.Status()
	s.True(started)
	s.False(stopped)
	started, stopped = cc2.lifecycle.Status()
	s.True(started)
	s.False(stopped)

	s.consumer.Stop()
	select {
	case <-s.consumer.Closed():
	case <-time.After(time.Millisecond):
		s.Fail("Consumer should be closed")
	}
}

func (s *MultiClusterConsumerTestSuite) TestStartError() {
	cc1 := NewClusterConsumer("cc1", newMockSaramaConsumer(), make(map[string]*TopicConsumer), tally.NoopScope, zap.L())
	cc2 := NewClusterConsumer("cc2", newMockSaramaConsumer(), make(map[string]*TopicConsumer), tally.NoopScope, zap.L())
	s.consumer.clusterConsumerMap["cc1"] = cc1
	s.consumer.clusterConsumerMap["cc2"] = cc2

	cc1.Start()
	cc1.Stop()

	s.Error(s.consumer.Start())
}
