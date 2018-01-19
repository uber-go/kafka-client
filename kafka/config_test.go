package kafka

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

type (
	ConsumerConfigTestSuite struct {
		suite.Suite
		config ConsumerConfig
		topic1 ConsumerTopic
		topic2 ConsumerTopic
	}
)

func TestConsumerConfigTestSuite(t *testing.T) {
	suite.Run(t, new(ConsumerConfigTestSuite))
}

func (s *ConsumerConfigTestSuite) SetupTest() {
	s.topic1 = ConsumerTopic{
		Topic: Topic{
			Name:    "topic1",
			Cluster: "cluster1",
			BrokerList: []string{
				"broker1",
				"broker2",
			},
		},
		DLQ: Topic{
			Name:    "dlq_topic1",
			Cluster: "dlq_cluster1",
			BrokerList: []string{
				"dlq_broker1",
				"dlq_broker2",
			},
		},
	}
	s.topic2 = ConsumerTopic{
		Topic: Topic{
			Name:    "topic2",
			Cluster: "cluster2",
			BrokerList: []string{
				"broker1",
				"broker2",
			},
		},
		DLQ: Topic{
			Name:    "dlq_topic2",
			Cluster: "dlq_cluster1",
			BrokerList: []string{
				"dlq_broker1",
				"dlq_broker2",
			},
		},
	}
	s.config = ConsumerConfig{
		GroupName: "groupName",
		TopicList: []ConsumerTopic{s.topic1, s.topic2},
	}
}

func (s *ConsumerConfigTestSuite) TestFilterByClusterTopic() {
	topic, err := s.config.TopicList.GetConsumerTopicByClusterTopic("cluster1", "topic1")
	s.NoError(err)
	s.Equal(s.topic1, topic)

	_, err = s.config.TopicList.GetConsumerTopicByClusterTopic("cluster3", "topic1")
	s.Error(err)
	_, err = s.config.TopicList.GetConsumerTopicByClusterTopic("cluster1", "topic3")
	s.Error(err)
}

func (s *ConsumerConfigTestSuite) TestHashKey() {
	s.Equal(Topic{
		Name:    "topic1",
		Cluster: "cluster1",
	}.HashKey(), s.topic1.HashKey())

	s.NotEqual(Topic{
		Name:    "topic1",
		Cluster: "cluster2",
	}.HashKey(), s.topic1.HashKey())
}
