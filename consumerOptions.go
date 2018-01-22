package kafkaclient

import (
	"github.com/uber-go/kafka-client/internal/consumer"
	"github.com/uber-go/kafka-client/kafkacore"
)

type (
	// ConsumerOption is the type for optional arguments to the NewConsumer constructor.
	ConsumerOption interface {
		apply(*consumer.Options)
	}

	consumerLimitOption struct {
		limits map[consumer.TopicPartition]int64
	}

	inboundMessageTransformerOption struct {
		transformer kafkacore.MessageTransformer
	}

	outboundMessageTransformerOption struct {
		transformer kafkacore.MessageTransformer
	}
)

// WithConsumerLimits sets consumer limits for a consumer.
// If consumer limits are set, the consumer will only consume messages from the specified topic-partitions
// up to the limits then shut itself down.
// If limits are set and the consumer is assigned a topic-partition that is not in the limits map, no messages
// will be received for that topic partition.
func WithConsumerLimits(limits map[string]map[int32]int64) ConsumerOption {
	topicPartitionLimits := make(map[consumer.TopicPartition]int64)
	for topic, partitionMap := range limits {
		for partition, offset := range partitionMap {
			topicPartitionLimits[consumer.TopicPartition{Topic: topic, Partition: partition}] = offset
		}
	}
	return &consumerLimitOption{
		limits: topicPartitionLimits,
	}
}

func (c consumerLimitOption) apply(opts *consumer.Options) {
	opts.Limits = consumer.NewTopicPartitionLimitMap(c.limits)
}

// WithInboundMessageTransformer sets a custom MessageTransformer for inbound messages
func WithInboundMessageTransformer(msgFactory kafkacore.MessageTransformer) ConsumerOption {
	return &inboundMessageTransformerOption{
		transformer: msgFactory,
	}
}

func (c inboundMessageTransformerOption) apply(options *consumer.Options) {
	options.InboundMessageTransformer = c.transformer
}

// WithOutboundMessageTransformer sets a custom MessageTransformer for outbound messages
func WithOutboundMessageTransformer(msgFactory kafkacore.MessageTransformer) ConsumerOption {
	return &outboundMessageTransformerOption{
		transformer: msgFactory,
	}
}

func (c outboundMessageTransformerOption) apply(options *consumer.Options) {
	options.OutboundMessageTransformer = c.transformer
}
