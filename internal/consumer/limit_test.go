package consumer

import (
	"github.com/stretchr/testify/suite"
	"testing"
)

type TopicPartitionLimitMapTestSuite struct {
	suite.Suite
	limitMap   TopicPartitionLimitMap
	noLimitMap TopicPartitionLimitMap
}

func TestTopicPartitionLimitMap(t *testing.T) {
	suite.Run(t, new(TopicPartitionLimitMapTestSuite))
}

func (s *TopicPartitionLimitMapTestSuite) SetupTest() {
	s.limitMap = NewTopicPartitionLimitMap(map[TopicPartition]int64{
		{Topic: "t", Partition: 0}: 100,
	})
	s.noLimitMap = NewTopicPartitionLimitMap(nil)
}

func (s *TopicPartitionLimitMapTestSuite) TestHasLimits() {
	s.True(s.limitMap.HasLimits())
	s.False(s.noLimitMap.HasLimits())
}

func (s *TopicPartitionLimitMapTestSuite) TestGet() {
	s.EqualValues(noLimit, s.noLimitMap.Get(TopicPartition{Topic: "t", Partition: 1}))
	s.EqualValues(defaultLimit, s.limitMap.Get(TopicPartition{Topic: "t", Partition: 1}))
	s.EqualValues(100, s.limitMap.Get(TopicPartition{Topic: "t", Partition: 0}))
}
