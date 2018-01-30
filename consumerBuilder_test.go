package kafkaclient

import (
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
		config                    *kafka.ConsumerConfig
		opts                      *consumer.Options
		builder                   *consumerBuilder
		saramaConsumerConstructor *saramaConsumerConstructorMock
		saramaProducerConstructor *saramaProducerConstructorMock
		resolverMock              *resolverMock
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

	s.builder = newConsumerBuilder(
		zap.NewNop(),
		tally.NoopScope,
		s.config,
		s.opts,
	)
}

func (s *ConsumerBuilderTestSuite) TestResolveBroker() {
	topicList := s.config.TopicList
	s.builder.resolveBrokers(s.resolverMock)
	s.NoError(s.builder.buildErrors.ToError())
	s.Equal(topicList, s.builder.topics)
}

func (s *ConsumerBuilderTestSuite) TestBuildSaramaConsumer() {
	s.builder.buildSaramaConsumerMap(s.saramaConsumerConstructor.f)
	s.NoError(s.builder.buildErrors.ToError())
}

func (s *ConsumerBuilderTestSuite) TestBuildSaramaProducerMap() {
	s.builder.topics = append(s.builder.topics, kafka.ConsumerTopic{Topic: kafka.Topic{Name: "topic3", Cluster: "cluster1", BrokerList: nil}, DLQ: kafka.Topic{Name: "", Cluster: "", BrokerList: nil}})
	s.builder.buildSaramaProducerMap(s.saramaProducerConstructor.f)
	s.NoError(s.builder.buildErrors.ToError())
}
