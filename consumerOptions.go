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
	"github.com/uber-go/kafka-client/internal/consumer"
	"github.com/uber-go/kafka-client/kafka"
	"crypto/tls"
)

type (
	// ConsumerOption is the type for optional arguments to the NewConsumer constructor.
	ConsumerOption interface {
		apply(*consumer.Options)
	}

	rangeConsumersOption struct {
		topicList kafka.ConsumerTopicList
	}

	dlqTopicsOptions struct {
		topicList kafka.ConsumerTopicList
	}

	retryTopicsOptions struct {
		topicList kafka.ConsumerTopicList
	}

	tlsOption struct {
		enable bool
		config *tls.Config
	}
)

// WithRangeConsumers creates a range consumer for the specified consumer topics.
// DEPRECATED
func WithRangeConsumers(topicList kafka.ConsumerTopicList) ConsumerOption {
	return &rangeConsumersOption{
		topicList: topicList,
	}
}

func (o *rangeConsumersOption) apply(opts *consumer.Options) {
	for _, topic := range o.topicList {
		opts.OtherConsumerTopics = append(opts.OtherConsumerTopics, consumer.Topic{
			ConsumerTopic:            topic,
			DLQMetadataDecoder:       consumer.NoopDLQMetadataDecoder,
			PartitionConsumerFactory: consumer.NewRangePartitionConsumer,
		})
	}
}

// WithDLQTopics creates a range consumer for the specified consumer DLQ topics.
func WithDLQTopics(topicList kafka.ConsumerTopicList) ConsumerOption {
	return &dlqTopicsOptions{
		topicList: topicList,
	}
}

func (o *dlqTopicsOptions) apply(opts *consumer.Options) {
	for _, topic := range o.topicList {
		opts.OtherConsumerTopics = append(opts.OtherConsumerTopics, consumer.Topic{
			ConsumerTopic:            topic,
			DLQMetadataDecoder:       consumer.ProtobufDLQMetadataDecoder,
			PartitionConsumerFactory: consumer.NewRangePartitionConsumer,
		})
	}
}

// WithRetryTopics creates a consumer for the specified consumer Retry topics.
func WithRetryTopics(topicList kafka.ConsumerTopicList) ConsumerOption {
	return &retryTopicsOptions{
		topicList: topicList,
	}
}

func (o *retryTopicsOptions) apply(opts *consumer.Options) {
	for _, topic := range o.topicList {
		opts.OtherConsumerTopics = append(opts.OtherConsumerTopics, consumer.Topic{
			ConsumerTopic:            topic,
			DLQMetadataDecoder:       consumer.ProtobufDLQMetadataDecoder,
			PartitionConsumerFactory: consumer.NewPartitionConsumer,
		})
	}
}

// WithTLS enables TLS for broker communication with the provided tls.Config.
func WithTLS(config *tls.Config) ConsumerOption {
	return &tlsOption{
		enable: true,
		config: config,
	}
}

func (o *tlsOption) apply(opts *consumer.Options) {
	opts.TLS.Enable = o.enable
	opts.TLS.Config = o.config
}
