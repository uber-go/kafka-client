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
	"sort"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/kafka-client/internal/consumer"
	"github.com/uber-go/kafka-client/kafka"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type (
	ConsumerBuilderTestSuite struct {
		suite.Suite
		builder                       *consumerBuilder
		config                        *kafka.ConsumerConfig
		resolver                      kafka.NameResolver
		mockSaramaClientConstructor   *mockSaramaClientConstructor
		mockSaramaConsumerConstructor *mockSaramaConsumerConstructor
		mockSaramaProducerConstructor *mockSaramaProducerConstructor
	}

	mockSaramaConsumerConstructor struct {
		clusterTopicMap map[string][]string
	}
	mockSaramaClientConstructor struct {
		clusters map[string]bool
	}
	mockSaramaProducerConstructor struct{}
)

func TestConsumerBuilderTestSuite(t *testing.T) {
	suite.Run(t, new(ConsumerBuilderTestSuite))
}

func (s *ConsumerBuilderTestSuite) SetupTest() {
	s.config = kafka.NewConsumerConfig("consumergroup", []kafka.ConsumerTopic{
		{
			Topic: kafka.Topic{
				Name:    "topic",
				Cluster: "cluster",
			},
			RetryQ: kafka.Topic{
				Name:    "retry-topic",
				Cluster: "dlq-cluster",
				Delay:   time.Microsecond,
			},
			DLQ: kafka.Topic{
				Name:    "dlq-topic",
				Cluster: "dlq-cluster",
			},
			MaxRetries: 1,
		},
		{
			Topic: kafka.Topic{
				Name:    "topic1",
				Cluster: "cluster",
			},
			RetryQ:     kafka.Topic{},
			DLQ:        kafka.Topic{},
			MaxRetries: 1,
		},
	})
	s.resolver = kafka.NewStaticNameResolver(
		map[string][]string{
			"topic":       {"cluster"},
			"retry-topic": {"dlq-cluster"},
			"dlq-topic":   {"dlq-cluster"},
		},
		map[string][]string{
			"cluster":     {"broker1:9092"},
			"dlq-cluster": {"broker2:9092"},
		},
	)
	s.builder = newConsumerBuilder(
		s.config,
		s.resolver,
		tally.NoopScope,
		zap.NewNop(),
	)
	s.mockSaramaConsumerConstructor = &mockSaramaConsumerConstructor{
		clusterTopicMap: make(map[string][]string),
	}
	s.mockSaramaClientConstructor = &mockSaramaClientConstructor{
		clusters: make(map[string]bool),
	}
	s.mockSaramaProducerConstructor = &mockSaramaProducerConstructor{}
	s.builder.constructors = consumer.Constructors{
		NewSaramaProducer: s.mockSaramaProducerConstructor.f,
		NewSaramaConsumer: s.mockSaramaConsumerConstructor.f,
		NewSaramaClient:   s.mockSaramaClientConstructor.f,
	}
}

func (s *ConsumerBuilderTestSuite) TestBuild() {
	consumer, err := s.builder.build()
	s.NoError(err)
	s.NotNil(consumer)
	// 3 clusters used in consumer
	s.Equal([]string{"cluster", "dlq-cluster"}, func() []string {
		output := make([]string, 0, 3)
		for cluster := range s.builder.clusterTopicsMap {
			output = append(output, cluster.name)
		}
		sort.Strings(output)
		return output
	}())
	// 2 sarama clients corresponding DLQ producers for 2 clusters
	s.Equal([]string{"dlq-cluster"}, func() []string {
		output := make([]string, 0, 2)
		for cluster := range s.builder.clusterSaramaClientMap {
			output = append(output, cluster)
		}
		sort.Strings(output)
		return output
	}())
	// consuming from 3 clusters
	s.Equal([]string{"cluster", "dlq-cluster"}, func() []string {
		output := make([]string, 0, 3)
		for cluster := range s.builder.clusterSaramaConsumerMap {
			output = append(output, cluster)
		}
		sort.Strings(output)
		return output
	}())
	// DLQ producers for 2 topics in 1 cluster
	s.Equal([]string{"dlq-topic", "retry-topic"}, func() []string {
		output := make([]string, 0, 2)
		for topic := range s.builder.clusterTopicSaramaProducerMap["dlq-cluster"] {
			output = append(output, topic)
		}
		sort.Strings(output)
		return output
	}())
	// make sure retry topic with delay gets populated to the read topic list
	s.Equal([]string{"retry-topic"}, func() []string {
		output := make([]string, 0, 1)
		for _, topicList := range s.builder.clusterTopicsMap {
			for _, topic := range topicList {
				if topic.Delay > 0 {
					output = append(output, topic.Name)
				}
			}
		}
		sort.Strings(output)
		return output
	}())
	// saramaClient constructor called for dlq-cluster = broker2:9092
	s.Equal([]string{"broker2:9092"}, func() []string {
		output := make([]string, 0, 1)
		for broker := range s.mockSaramaClientConstructor.clusters {
			output = append(output, broker)
		}
		sort.Strings(output)
		return output
	}())
	// Sarama consumer constructor called for broker1:9092 and broker2:9092 clusters
	s.Equal([]string{"broker1:9092", "broker2:9092"}, func() []string {
		output := make([]string, 0, 2)
		for broker := range s.mockSaramaConsumerConstructor.clusterTopicMap {
			output = append(output, broker)
		}
		sort.Strings(output)
		return output
	}())
}

func (s *ConsumerBuilderTestSuite) TestBuildConsumersWithCommitDisabled() {
	s.builder.kafkaConfig.Offsets.Commits.Enabled = false
	consumer, err := s.builder.build()
	s.NoError(err)
	s.NotNil(consumer)
}

func (m *mockSaramaConsumerConstructor) f(brokers []string, _ string, topics []string, _ *cluster.Config) (consumer.SaramaConsumer, error) {
	m.clusterTopicMap[brokers[0]] = topics
	return nil, nil
}

func (m *mockSaramaClientConstructor) f(brokers []string, _ *sarama.Config) (sarama.Client, error) {
	m.clusters[brokers[0]] = true
	return nil, nil
}

func (m *mockSaramaProducerConstructor) f(sarama.Client) (sarama.AsyncProducer, error) {
	return nil, nil
}
