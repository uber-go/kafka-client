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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
	"github.com/uber-go/kafka-client/internal/consumer"
	"github.com/uber-go/kafka-client/kafka"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type (
	ConsumerBuilderTestSuite struct {
		suite.Suite
		config                     *kafka.ConsumerConfig
		opts                       *consumer.Options
		builder                    *consumerBuilder
		saramaConsumerConstructor  *saramaConsumerConstructorMock
		saramaProducerConstructor  *saramaProducerConstructorMock
		clusterConsumerConstructor *clusterConsumerConstructorMock
		resolverMock               *resolverMock
	}
)

func TestConsumerBuilderTestSuite(t *testing.T) {
	suite.Run(t, new(ConsumerBuilderTestSuite))
}

func (s *ConsumerBuilderTestSuite) SetupTest() {
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
			{
				Topic: kafka.Topic{
					Name:       "topic3",
					Cluster:    "bad-cluster",
					BrokerList: []string{},
				},
				DLQ: kafka.Topic{
					Name:       "dlq-topic3",
					Cluster:    "bad-cluster",
					BrokerList: []string{},
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
	opts := buildOptions(s.config)
	s.opts = opts

	s.saramaConsumerConstructor = &saramaConsumerConstructorMock{errRet: make(map[string]error)}
	s.saramaProducerConstructor = &saramaProducerConstructorMock{errRet: make(map[string]error)}
	s.clusterConsumerConstructor = &clusterConsumerConstructorMock{errRet: make(map[string]error)}

	s.builder = newConsumerBuilder(
		zap.NewNop(),
		tally.NoopScope,
		s.config,
		s.opts,
		nil,
	)
}

func (s *ConsumerBuilderTestSuite) TestResolveBroker() {
	s.resolverMock.errs["bad-cluster"] = errors.New("error")
	topicList := s.config.TopicList[:2]
	s.builder.resolveBrokers(s.resolverMock)
	s.Error(s.builder.buildErrors.ToError())
	s.Equal(topicList, s.builder.topics)
}

func (s *ConsumerBuilderTestSuite) TestBuildSaramaConsumer() {
	s.saramaConsumerConstructor.errRet[""] = errors.New("error")
	s.builder.buildSaramaConsumerMap(s.saramaConsumerConstructor.f)
	s.Error(s.builder.buildErrors.ToError())
	s.Equal(1, len(s.builder.clusterToSaramaConsumer))
	_, ok := s.builder.clusterToSaramaConsumer["production-cluster"]
	s.True(ok)
}

func (s *ConsumerBuilderTestSuite) TestBuildSaramaProducerMap() {
	s.saramaProducerConstructor.errRet[""] = errors.New("error")
	s.builder.buildSaramaProducerMap(s.saramaProducerConstructor.f)
	s.Error(s.builder.buildErrors.ToError())
	s.Equal(1, len(s.builder.dlqClusterToSaramaProducer))
	_, ok := s.builder.dlqClusterToSaramaProducer["dlq-cluster"]
	s.True(ok)
}

func (s *ConsumerBuilderTestSuite) TestBuildClusterConsumerMap() {
	s.builder.resolveBrokers(s.resolverMock)
	s.builder.buildSaramaConsumerMap(s.saramaConsumerConstructor.f)
	s.builder.buildSaramaProducerMap(s.saramaProducerConstructor.f)
	s.NoError(s.builder.buildClusterConsumerMap(s.clusterConsumerConstructor.f))
	s.Error(s.builder.buildErrors.ToError())
}

func (s *ConsumerBuilderTestSuite) TestBuild() {
	// no construction error and partial construction disabled
	_, err := s.builder.build()
	s.NoError(err)

	// construction error and partial construction enabled
	partialConstructionOpt := EnablePartialConsumption()
	s.builder.buildErrors = &consumerErrorList{
		errs: []ConsumerError{{Topic: kafka.ConsumerTopic{}, error: errors.New("error")}},
	}
	buildOpts := []ConsumerOption{partialConstructionOpt}
	s.builder.buildOpts = buildOpts
	_, err = s.builder.build()
	s.NoError(err)

	pc, ok := partialConstructionOpt.(*partialConsumption)
	s.True(ok)
	s.Equal(1, len(pc.errs.errs))
}
