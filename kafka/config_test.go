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
		},
		DLQ: Topic{
			Name:    "dlq_topic1",
			Cluster: "dlq_cluster1",
		},
	}
	s.topic2 = ConsumerTopic{
		Topic: Topic{
			Name:    "topic2",
			Cluster: "cluster2",
		},
		DLQ: Topic{
			Name:    "dlq_topic2",
			Cluster: "dlq_cluster1",
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
