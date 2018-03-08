package kafkaclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/kafka-client/internal/consumer"
	"github.com/uber-go/kafka-client/kafka"
)

func TestDLQConsumerOptions(t *testing.T) {
	testTopics := make([]kafka.ConsumerTopic, 0, 10)
	testTopics = append(testTopics, *new(kafka.ConsumerTopic))
	testTopics = append(testTopics, *new(kafka.ConsumerTopic))
	consumerOption := WithDLQTopics(testTopics)
	options := consumer.DefaultOptions()
	consumerOption.apply(options)
	assert.Equal(t, 2, len(options.OtherConsumerTopics))
	assert.Equal(t, consumer.TopicTypeDLQ, options.OtherConsumerTopics[0].TopicType)
}

func TestRetryConsumerOptions(t *testing.T) {
	testTopics := make([]kafka.ConsumerTopic, 0, 10)
	testTopics = append(testTopics, *new(kafka.ConsumerTopic))
	testTopics = append(testTopics, *new(kafka.ConsumerTopic))
	consumerOption := WithRetryTopics(testTopics)
	options := consumer.DefaultOptions()
	consumerOption.apply(options)
	assert.Equal(t, 2, len(options.OtherConsumerTopics))
	assert.Equal(t, consumer.TopicTypeRetryQ, options.OtherConsumerTopics[0].TopicType)
}
