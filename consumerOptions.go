package kafkaclient

import (
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/uber-go/kafka-client/internal/consumer"
	"time"
)

// ConsumerOption is the type for optional arguments to the NewConsumer constructor.
type (
	ConsumerOption interface {
		apply(*consumer.Options)
	}
	consumerLimitOption struct {
		limits map[consumer.TopicPartition]int64
	}
)

var defaultOptions = consumer.Options{
	Concurrency:            1024,
	RcvBufferSize:          2 * 1024, // twice the concurrency for compute/io overlap
	PartitionRcvBufferSize: 32,
	OffsetCommitInterval:   time.Second,
	RebalanceDwellTime:     time.Second,
	MaxProcessingTime:      250 * time.Millisecond,
	OffsetPolicy:           sarama.OffsetOldest,
	ConsumerMode:           cluster.ConsumerModePartitions,
	Limits:                 nil,
}

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
	opts.Limits = c.limits
}
