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
	"time"

	"github.com/Shopify/sarama"
	"github.com/uber-go/kafka-client/internal/backoff"
	"github.com/uber-go/kafka-client/kafka"
	"github.com/uber-go/kafka-client/kafkacore"
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

	dlqWithRetry struct {
		retryCount int64
		retry      DLQ
		dlq        DLQ
		tally      tally.Scope
		logger     *zap.Logger
	}

	retryImpl struct {
		msgCh chan kafka.Message
	}

	errorTopic struct {
		topic          string
		producer       sarama.SyncProducer
		retryPolicy    backoff.RetryPolicy
		msgTransformer kafkacore.MessageTransformer
		tally          tally.Scope
		logger         *zap.Logger
	}

	dlqNoop struct{}
)

const (
	dlqRetryInitialInterval = 200 * time.Millisecond
	dlqRetryMaxInterval     = 10 * time.Second
)

// NewDLQWithRetry returns a DLQ writer with in-memory retry.
// Messages that are added to the DLQ will be redelivered on the message channel up to retryCount
// number of times.
func NewDLQWithRetry(retry, dlq DLQ, retryCount int64, scope tally.Scope, logger *zap.Logger) DLQ {
	return &dlqWithRetry{
		retryCount: retryCount,
		retry:      retry,
		dlq:        dlq,
		tally:      scope,
		logger:     logger,
	}
}

// NewDLQ returns a DLQ writer for writing to a specific DLQ topic
func NewDLQ(topic, cluster string, producer sarama.SyncProducer, msgTransformer kafkacore.MessageTransformer, scope tally.Scope, logger *zap.Logger) DLQ {
	return &errorTopic{
		topic:          topic,
		producer:       producer,
		retryPolicy:    newDLQRetryPolicy(),
		msgTransformer: msgTransformer,
		tally:          scope,
		logger:         logger.With(zap.String("topic", topic), zap.String("cluster", cluster)),
	}
}

// NewRetry is an implementation of a DLQ writer in which adding a message to the DLQ simply
// results in the message being redelivered on the Message channel with incremented retry count.
func NewRetry(msgCh chan kafka.Message) DLQ {
	return &retryImpl{
		msgCh: msgCh,
	}
}

func (h *dlqWithRetry) Close() {
	h.retry.Close()
	h.dlq.Close()
}

func (h *dlqWithRetry) Add(m kafka.Message) error {
	retryCount := m.RetryCount() + 1
	if retryCount > h.retryCount {
		return h.dlq.Add(m)
	}
	return h.retry.Add(m)
}

func (r *retryImpl) Close() {}

func (r *retryImpl) Add(m kafka.Message) error {
	outM := newRetryMessage(m, m.RetryCount()+1)
	r.msgCh <- outM
	return nil
}

// Add blocks until successfully enqueuing the given
// message into the error queue
func (d *errorTopic) Add(m kafka.Message) error {
	sm := d.newSaramaMessage(m)
	return backoff.Retry(func() error { return d.add(sm) }, d.retryPolicy, d.isRetryable)
}

func (d *errorTopic) Close() {
	d.producer.Close()
}

func (d *errorTopic) add(m *sarama.ProducerMessage) error {
	_, _, err := d.producer.SendMessage(m)
	if err != nil {
		d.logger.Error("error sending message to DLQ", zap.Error(err))
	}
	return err
}

func (d *errorTopic) isRetryable(err error) bool {
	return true
}

func newDLQRetryPolicy() backoff.RetryPolicy {
	policy := backoff.NewExponentialRetryPolicy(dlqRetryInitialInterval)
	policy.SetMaximumInterval(dlqRetryMaxInterval)
	policy.SetExpirationInterval(backoff.NoInterval)
	return policy
}

func (d *errorTopic) newSaramaMessage(m kafkacore.Message) *sarama.ProducerMessage {
	outM := d.msgTransformer.Transform(m)
	return &sarama.ProducerMessage{
		Topic: d.topic,
		Key:   sarama.StringEncoder(outM.Key()),
		Value: sarama.StringEncoder(outM.Value()),
	}
}

// NewDLQNoop returns a DLQ that drops everything on the floor
// this is used only when DLQ is not configured for a consumer
func NewDLQNoop() DLQ {
	return &dlqNoop{}
}
func (d *dlqNoop) Add(m kafka.Message) error { return nil }
func (d *dlqNoop) Close()                    {}
