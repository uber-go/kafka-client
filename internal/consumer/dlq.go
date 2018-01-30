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
	"bytes"
	"time"

	"github.com/Shopify/sarama"
	"github.com/uber-go/kafka-client/internal/avro"
	"github.com/uber-go/kafka-client/internal/backoff"
	"github.com/uber-go/kafka-client/kafka"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type (
	// DLQ is the interface for implementations that
	// can take a message and put them into some sort
	// of error queue for later processing
	DLQ interface {
		Close()
		// Add adds the given message to DLQ
		Add(m kafka.Message) error
	}
	dlqImpl struct {
		topic       string
		producer    sarama.SyncProducer
		retryPolicy backoff.RetryPolicy
		tally       tally.Scope
		logger      *zap.Logger
	}
	dlqNoop struct{}
)

const (
	dlqRetryInitialInterval = 200 * time.Millisecond
	dlqRetryMaxInterval     = 10 * time.Second
)

// NewDLQ returns a DLQ writer for writing to a specific DLQ topic
func NewDLQ(topic, cluster string, producer sarama.SyncProducer, scope tally.Scope, logger *zap.Logger) DLQ {
	return &dlqImpl{
		topic:       topic,
		producer:    producer,
		retryPolicy: newDLQRetryPolicy(),
		tally:       scope,
		logger:      logger.With(zap.String("topic", topic), zap.String("cluster", cluster)),
	}
}

// Add blocks until successfully enqueuing the given
// message into the error queue
func (d *dlqImpl) Add(m kafka.Message) error {
	sm, err := d.newSaramaMessage(m)
	if err != nil {
		return err
	}
	return backoff.Retry(func() error { return d.add(sm) }, d.retryPolicy, d.isRetryable)
}

func (d *dlqImpl) Close() {
	d.producer.Close()
}

func (d *dlqImpl) add(m *sarama.ProducerMessage) error {
	_, _, err := d.producer.SendMessage(m)
	if err != nil {
		d.logger.Error("error sending message to DLQ", zap.Error(err))
	}
	return err
}

func (d *dlqImpl) isRetryable(err error) bool {
	return true
}

func newDLQRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(dlqRetryInitialInterval)
	policy.SetMaximumInterval(dlqRetryMaxInterval)
	policy.SetExpirationInterval(backoff.NoInterval)
	return policy
}

func (d *dlqImpl) newSaramaMessage(m kafka.Message) (*sarama.ProducerMessage, error) {
	key, err := d.encodeDLQMetadata(m)
	if err != nil {
		return nil, err
	}
	return &sarama.ProducerMessage{
		Topic: d.topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(m.Value()),
	}, nil
}

func (d *dlqImpl) encodeDLQMetadata(m kafka.Message) ([]byte, error) {
	key := new(bytes.Buffer)
	metadata := &avro.DlqMetadata{
		RetryCount: 0,
		Data:       m.Key(),
		Topic:      m.Topic(),
		Partition:  int64(m.Partition()),
		Offset:     m.Offset(),
		TimestampSec:  m.Timestamp().Unix(),
	}
	if err := metadata.Serialize(key); err != nil {
		return nil, err
	}
	return key.Bytes(), nil
}

// NewDLQNoop returns a DLQ that drops everything on the floor
// this is used only when DLQ is not configured for a consumer
func NewDLQNoop() DLQ {
	return &dlqNoop{}
}
func (d *dlqNoop) Add(m kafka.Message) error { return nil }
func (d *dlqNoop) Close()                    {}
