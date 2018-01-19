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

package kafkaclient

import (
	"testing"
	"time"

	"fmt"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/kafka-client/internal/consumer"
	"github.com/uber-go/kafka-client/kafka"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type (
	ClientTestSuite struct {
		suite.Suite
		config                    *kafka.ConsumerConfig
		client                    *Client
		saramaConsumerConstructor *saramaConsumerConstructorMock
		saramaProducerConstructor *saramaProducerConstructorMock
		resolverMock              *resolverMock
	}

	resolverMock struct {
		errs        map[string]error
		clusterToIP map[string][]string
	}

	saramaConsumerConstructorMock struct {
		errRet map[string]error
	}

	saramaProducerConstructorMock struct {
		errRet map[string]error
	}

	clusterConsumerConstructorMock struct {
		errRet map[string]error
	}
)

func (m *saramaConsumerConstructorMock) f(brokers []string, _ string, _ []string, _ *cluster.Config) (sc consumer.SaramaConsumer, err error) {
	key := ""
	for _, broker := range brokers {
		key += broker
	}

	err = m.errRet[key]
	return
}

func (m *saramaProducerConstructorMock) f(brokers []string) (p sarama.SyncProducer, err error) {
	key := ""
	for _, broker := range brokers {
		key += broker
	}

	err = m.errRet[key]
	return
}

func (m *resolverMock) ResolveIPForCluster(cluster string) (ip []string, err error) {
	err = m.errs[cluster]
	ip = m.clusterToIP[cluster]
	return
}

func (m *resolverMock) ResolveClusterForTopic(topic string) (cluster []string, err error) {
	err = m.errs[topic]
	return
}

func TestClientTestSuite(t *testing.T) {
	suite.Run(t, new(ClientTestSuite))
}

func (s *ClientTestSuite) SetupTest() {
	s.config = &kafka.ConsumerConfig{
		TopicList: []kafka.ConsumerTopic{
			{
				Topic: kafka.Topic{
					Name:       "topic1",
					Cluster:    "production-cluster",
					BrokerList: []string{"b1"},
				},
				DLQ: kafka.Topic{
					Name:       "dlq-topic2",
					Cluster:    "dlq-cluster",
					BrokerList: []string{"d1"},
				},
			},
			{
				Topic: kafka.Topic{
					Name:       "topic2",
					Cluster:    "production-cluster",
					BrokerList: []string{"b1"},
				},
				DLQ: kafka.Topic{
					Name:       "dlq-topic2",
					Cluster:    "dlq-cluster",
					BrokerList: []string{"d1"},
				},
			},
		},
		GroupName:   "unit-test-cg",
		Concurrency: 4,
	}

	s.resolverMock = &resolverMock{
		errs: map[string]error{
			"bad-cluster": fmt.Errorf("error"),
		},
		clusterToIP: map[string][]string{
			"production-cluster":   {"b1"},
			"production-cluster-2": {"b2"},
			"dlq-cluster":          {"d1"},
			"dlq-cluster-2":        {"d2"},
		},
	}

	s.saramaConsumerConstructor = &saramaConsumerConstructorMock{errRet: make(map[string]error)}
	s.saramaProducerConstructor = &saramaProducerConstructorMock{errRet: make(map[string]error)}

	s.client = &Client{
		tally:                         tally.NoopScope,
		logger:                        zap.NewNop(),
		resolver:                      s.resolverMock,
		saramaSyncProducerConstructor: s.saramaProducerConstructor.f,
		saramaConsumerConstructor:     s.saramaConsumerConstructor.f,
	}
}

func (s *ClientTestSuite) TestBuildSaramaConfig() {
	opts := &consumer.Options{
		RcvBufferSize:          128,
		PartitionRcvBufferSize: 64,
		OffsetCommitInterval:   time.Minute,
		RebalanceDwellTime:     time.Hour,
		MaxProcessingTime:      time.Second,
		OffsetPolicy:           sarama.OffsetNewest,
		ConsumerMode:           cluster.ConsumerModePartitions,
	}
	config := buildSaramaConfig(opts)
	s.Equal(opts.PartitionRcvBufferSize, config.ChannelBufferSize)
	s.Equal(opts.OffsetPolicy, config.Consumer.Offsets.Initial)
	s.Equal(opts.OffsetCommitInterval, config.Consumer.Offsets.CommitInterval)
	s.Equal(opts.MaxProcessingTime, config.Consumer.MaxProcessingTime)
	s.True(config.Consumer.Return.Errors)
	s.Equal(opts.RebalanceDwellTime, config.Group.Offsets.Synchronization.DwellTime)
	s.True(config.Group.Return.Notifications)
	s.Equal(cluster.ConsumerModePartitions, config.Group.Mode)
}

func (s *ClientTestSuite) TestResolveBroker() {
	topicList := s.config.TopicList
	outputTopics, err := s.client.resolveBrokers(topicList)
	s.NoError(err)
	s.Equal(topicList, outputTopics)

	topicList = append(topicList, kafka.ConsumerTopic{Topic: kafka.Topic{Name: "t1", Cluster: "bad-cluster", BrokerList: nil}, DLQ: kafka.Topic{Name: "t1", Cluster: "", BrokerList: nil}})
	_, err = s.client.resolveBrokers(topicList)
	s.Error(err)
}

func (s *ClientTestSuite) TestBuildSaramaConsumer() {
	topicList := s.config.TopicList
	_, err := s.client.buildSaramaConsumer(topicList, s.config.GroupName, nil)
	s.NoError(err)

	_, err = s.client.buildSaramaConsumer(nil, s.config.GroupName, nil)
	s.Error(err)
}

func (s *ClientTestSuite) TestBuildSaramaProducerMap() {
	topicList := s.config.TopicList
	// Adding a topic with no DLQ set should not through error.
	// It is the users responsibility to ensure that they do not nack if they do not enable DLQ.
	topicList = append(topicList, kafka.ConsumerTopic{Topic: kafka.Topic{Name: "topic3", Cluster: "cluster1", BrokerList: nil}, DLQ: kafka.Topic{Name: "", Cluster: "", BrokerList: nil}})
	_, err := s.client.buildSaramaProducerMap(topicList)
	s.NoError(err)

	_, err = s.client.buildSaramaConsumer(nil, s.config.GroupName, nil)
	s.Error(err)
}

func (s *ClientTestSuite) TestValidateTopicListFromSingleCluster() {
	topicList := s.config.TopicList
	s.NoError(validateTopicListFromSingleCluster(topicList))
	s.Error(validateTopicListFromSingleCluster(nil))
	topicList = append(topicList, kafka.ConsumerTopic{Topic: kafka.Topic{Name: "foo", Cluster: "bar"}})
	s.Error(validateTopicListFromSingleCluster(topicList))
}
