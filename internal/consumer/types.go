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

package consumer

import (
	"errors"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/golang/protobuf/proto"
	"github.com/gig/kafka-client/internal/util"
	"github.com/gig/kafka-client/kafka"
	"go.uber.org/zap/zapcore"
)

type (
	// SaramaConsumer is an interface for external consumer library (sarama)
	SaramaConsumer interface {
		Close() error
		Errors() <-chan error
		Notifications() <-chan *cluster.Notification
		Partitions() <-chan cluster.PartitionConsumer
		CommitOffsets() error
		Messages() <-chan *sarama.ConsumerMessage
		HighWaterMarks() map[string]map[int32]int64
		MarkOffset(msg *sarama.ConsumerMessage, metadata string)
		MarkPartitionOffset(topic string, partition int32, offset int64, metadata string)
		ResetPartitionOffset(topic string, partition int32, offset int64, metadata string)
	}

	// saramaConsumer is an internal version of SaramaConsumer that implements a close method that can be safely called
	// multiple times.
	saramaConsumer struct {
		SaramaConsumer
		lifecycle *util.RunLifecycle
	}

	// saramaProducer is an internal version of SaramaConsumer that implements a close method that can be safely called
	// multiple times.
	saramaProducer struct {
		sarama.AsyncProducer
		lifecycle *util.RunLifecycle
	}

	// saramaClient is an internal version of sarama Client that implements a close method that can be safely called
	// multiple times
	saramaClient struct {
		sarama.Client
		lifecycle *util.RunLifecycle
	}

	// Constructors wraps multiple Sarama Constructors, which can be used for tests.
	Constructors struct {
		NewSaramaProducer func(sarama.Client) (sarama.AsyncProducer, error)
		NewSaramaConsumer func([]string, string, []string, *cluster.Config) (SaramaConsumer, error)
		NewSaramaClient   func([]string, *sarama.Config) (sarama.Client, error)
	}

	// Topic is an internal wrapper around kafka.ConsumerTopic
	Topic struct {
		kafka.ConsumerTopic
		DLQMetadataDecoder
		PartitionConsumerFactory
		ConsumerGroupSuffix string
	}

	// DLQMetadataDecoder decodes a byte array into DLQMetadata.
	DLQMetadataDecoder func([]byte) (*DLQMetadata, error)
)

// NoopDLQMetadataDecoder does no decoding and returns a default DLQMetadata object.
func NoopDLQMetadataDecoder(b []byte) (*DLQMetadata, error) {
	return newDLQMetadata(), nil
}

// ProtobufDLQMetadataDecoder uses proto.Unmarshal to decode protobuf encoded binary into the DLQMetadata object.
func ProtobufDLQMetadataDecoder(b []byte) (*DLQMetadata, error) {
	dlqMetadata := newDLQMetadata()
	if b == nil {
		return nil, errors.New("expected to decode non-nil byte array to DLQ metadata")
	}
	if err := proto.Unmarshal(b, dlqMetadata); err != nil {
		return nil, err
	}
	return dlqMetadata, nil
}

// MarshalLogObject implements zapcore.ObjectMarshaler for structured logging.
func (t Topic) MarshalLogObject(e zapcore.ObjectEncoder) error {
	t.ConsumerTopic.MarshalLogObject(e)
	return nil
}

// NewSaramaConsumer returns a new SaramaConsumer that has a Close method that can be called multiple times.
func NewSaramaConsumer(brokers []string, groupID string, topics []string, config *cluster.Config) (SaramaConsumer, error) {
	c, err := cluster.NewConsumer(brokers, groupID, topics, config)
	if err != nil {
		return nil, err
	}

	return newSaramaConsumer(c)
}

func newSaramaConsumer(c SaramaConsumer) (SaramaConsumer, error) {
	sc := saramaConsumer{
		SaramaConsumer: c,
		lifecycle:      util.NewRunLifecycle("sarama-consumer"),
	}

	sc.lifecycle.Start(func() error { return nil }) // must start lifecycle so stop will stop

	return &sc, nil
}

// Close overwrites the underlying SaramaConsumer Close method with one that can be safely called multiple times.
//
// This close will always return nil error.
func (p *saramaConsumer) Close() error {
	p.lifecycle.Stop(func() {
		p.SaramaConsumer.Close()
	})
	return nil
}

// NewSaramaProducer returns a new AsyncProducer that has Close method that can be called multiple times.
func NewSaramaProducer(client sarama.Client) (sarama.AsyncProducer, error) {
	p, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	return newSaramaProducer(p)
}

func newSaramaProducer(p sarama.AsyncProducer) (sarama.AsyncProducer, error) {
	sp := saramaProducer{
		AsyncProducer: p,
		lifecycle:     util.NewRunLifecycle("sarama-producer"),
	}

	sp.lifecycle.Start(func() error { return nil }) // must start lifecycle so stop will stop

	return &sp, nil
}

// Close overwrites the underlying Sarama Close method with one that can be safely called multiple times.
//
// This close will always return nil error.
func (p *saramaProducer) Close() error {
	p.lifecycle.Stop(func() {
		p.AsyncProducer.Close()
	})
	return nil
}

// NewSaramaClient returns an internal sarama Client, which can be safely closed multiple times.
func NewSaramaClient(brokers []string, config *sarama.Config) (sarama.Client, error) {
	sc, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, err
	}

	return newSaramaClient(sc)
}

func newSaramaClient(client sarama.Client) (sarama.Client, error) {
	c := &saramaClient{
		Client:    client,
		lifecycle: util.NewRunLifecycle("sarama-client"),
	}
	c.lifecycle.Start(func() error { return nil }) // must start lifecycle so stop will stop
	return c, nil
}

// Close overwrites the underlying Sarama Close method with one that can be safely called multiple times.
//
// This close will always return nil error.
func (c *saramaClient) Close() error {
	c.lifecycle.Stop(func() {
		c.Client.Close()
	})
	return nil
}
