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

import "time"

type (
	// Consumer is the interface for a kafka consumer
	Consumer interface {
		// Name returns the name of this consumer group.
		Name() string
		// Topics returns the names of the topics being consumed.
		Topics() []string
		// Start starts the consumer
		Start() error
		// Stop stops the consumer
		Stop()
		// Closed returns a channel which will be closed after this consumer is completely shutdown
		Closed() <-chan struct{}
		// Messages return the message channel for this consumer
		Messages() <-chan Message
	}

	// Message is the interface for a Kafka message
	Message interface {
		// Key is a mutable reference to the message's key.
		Key() []byte
		// Value is a mutable reference to the message's value.
		Value() []byte
		// Topic is the topic from which the message was read.
		Topic() string
		// Partition is the ID of the partition from which the message was read.
		Partition() int32
		// Offset is the message's offset.
		Offset() int64
		// Timestamp returns the timestamp for this message
		Timestamp() time.Time
		// Ack marks the message as successfully processed.
		Ack() error
		// Nack marks the message processing as failed and the message will be retried or sent to DLQ.
		Nack() error
	}

	// Client defines the contract for kafka client implementations
	Client interface {
		NewConsumer(config *ConsumerConfig) (Consumer, error)
	}

	// ClusterNameResolver is the interface for any implementation of
	// name resolver that can translate a specified cluster name to a
	// list of broker ip addresses
	ClusterNameResolver interface {
		// Resolve takes a cluster name and returns a list of
		// broker ip addresses for that topic
		Resolve(cluster string) ([]string, error)
	}
)
