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
	"github.com/gig/kafka-client/internal/consumer"
	"github.com/gig/kafka-client/kafka"
)

type (
	// ConsumerOption is the type for optional arguments to the NewConsumer constructor.
	ConsumerOption interface {
		apply(*consumer.Options)
	}

	dlqTopicsOptions struct {
		topicList kafka.ConsumerTopicList
	}

	retryTopicsOptions struct {
		topicList kafka.ConsumerTopicList
	}

	clientIDOptions struct {
		clientID string
	}

	SASLOptions struct {
		username string
		password string
	}
)

func WithSASLAuth(username, password string) ConsumerOption {
	return &SASLOptions{
		username: username,
		password: password,
	}
}

func (o *SASLOptions) apply(opts *consumer.Options) {
	opts.SASLEnabled = true
	opts.SASLUsername = o.username
	opts.SASLPassword = o.password
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
			ConsumerGroupSuffix:      consumer.DLQConsumerGroupNameSuffix,
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

// WithClientID sets client id.
func WithClientID(clientID string) ConsumerOption {
	return &clientIDOptions{
		clientID: clientID,
	}
}

func (o *clientIDOptions) apply(opts *consumer.Options) {
	opts.ClientID = o.clientID
}
