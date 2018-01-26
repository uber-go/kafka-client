package kafkaclient

import (
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/kafka-client/internal/consumer"
	"testing"
)

type ConsumerOptionsTestSuite struct {
	suite.Suite
	options *consumer.Options
}

func TestConsumerOptionsTestSuite(t *testing.T) {
	suite.Run(t, new(ConsumerOptionsTestSuite))
}

func (s *ConsumerOptionsTestSuite) SetupTest() {
	s.options = &defaultOptions
}

func (s *ConsumerOptionsTestSuite) TestWithConsumerLimits() {
	WithConsumerLimits(map[string]map[int32]int64{
		"topic": {
			0: 100,
		},
	}).apply(s.options)

	s.Equal(map[consumer.TopicPartition]int64{
		{Topic: "topic", Partition: 0}: 100,
	}, s.options.Limits)
}
