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
	"fmt"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"github.com/gig/kafka-client/internal/metrics"
	"github.com/gig/kafka-client/internal/util"
	"github.com/gig/kafka-client/kafka"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var (
	errShutdown = errors.New("error consumer shutdown")
	errNoDLQ    = errors.New("no persistent dlq configured")

	// RetryQErrorQType is the error queue for the retryQ.
	RetryQErrorQType ErrorQType = "retryQ"
	// DLQErrorQType is the error queue for DLQ.
	DLQErrorQType ErrorQType = "DLQ"

	// DLQConsumerGroupNameSuffix is the consumer group name used by the DLQ merge process.
	DLQConsumerGroupNameSuffix = "-dlq-merger"
)

type (
	// DLQ is the interface for implementations that
	// can take a message and put them into some sort
	// of error queue for later processing
	DLQ interface {
		// Start the DLQ producer
		Start() error
		// Stop the DLQ producer and close resources it holds.
		Stop()
		// Add adds the given message to DLQ.
		// This is a synchronous call and will block until sending is successful.
		Add(m kafka.Message, qTypes ...ErrorQType) error
	}

	// ErrorQType is the queue type to send messages to when using the DLQ interface.
	ErrorQType string

	// dlqMultiplexer is an implementation of DLQ which sends messages to a RetryQ for a configured number of times before
	// sending messages to thq DLQ.
	// Messages send to the RetryQ will be automatically reconsumed by the library.
	dlqMultiplexer struct {
		// retryCountThreshold is the threshold that is used to determine whether a message
		// goes to retryTopic or dlqTopic
		retryCountThreshold int64
		// retryTopic is topic that acts as a retry queue.
		// messages with retry count < the retry count threshold in the multiplexer will be sent to this topic.
		retryTopic DLQ
		// dlqTopic is topic that acts as a dead letter queue.
		// messages with retry count >= the retry count threshold in the multiplexer will be sent to this topic.
		dlqTopic DLQ
	}

	// bufferedErrorTopic is a client side abstraction for an error topic on the Kafka cluster.
	// This library uses the error topic as a base abstraction for retry and dlq topics.
	//
	// bufferedErrorTopic internally batches messages, so calls to Add may block until the internal
	// buffer is flushed and a batch of messages is send to the cluster.
	bufferedErrorTopic struct {
		topic    kafka.Topic
		producer sarama.AsyncProducer

		stopC chan struct{}
		doneC chan struct{}

		scope     tally.Scope
		logger    *zap.Logger
		lifecycle *util.RunLifecycle
	}

	noopDLQ struct{}
)

// NewBufferedDLQ returns a DLQ that is backed by a buffered async sarama producer.
func NewBufferedDLQ(topic kafka.Topic, producer sarama.AsyncProducer, scope tally.Scope, logger *zap.Logger) DLQ {
	return newBufferedDLQ(topic, producer, scope, logger.With(zap.String("topic", topic.Name), zap.String("cluster", topic.Cluster)))
}

func newBufferedDLQ(topic kafka.Topic, producer sarama.AsyncProducer, scope tally.Scope, logger *zap.Logger) *bufferedErrorTopic {
	scope = scope.Tagged(map[string]string{"topic": topic.Name, "cluster": topic.Cluster})
	logger = logger.With(
		zap.String("topic", topic.Name),
		zap.String("cluster", topic.Cluster),
	)
	return &bufferedErrorTopic{
		topic:     topic,
		producer:  producer,
		stopC:     make(chan struct{}),
		doneC:     make(chan struct{}),
		scope:     scope,
		logger:    logger,
		lifecycle: util.NewRunLifecycle(fmt.Sprintf("dlqProducer-%s-%s", topic.Name, topic.Cluster)),
	}
}

func (d *bufferedErrorTopic) Start() error {
	return d.lifecycle.Start(func() error {
		go d.asyncProducerResponseLoop()
		d.logger.Info("DLQ started")
		d.scope.Counter(metrics.KafkaDLQStarted).Inc(1)
		return nil
	})
}

func (d *bufferedErrorTopic) Stop() {
	d.lifecycle.Stop(func() {
		close(d.stopC)
		d.producer.Close()
		<-d.doneC
		d.logger.Info("DLQ stopped")
		d.scope.Counter(metrics.KafkaDLQStopped).Inc(1)
	})
}

// Add a message to the buffer for the error topic.
// ErrorQType is ignored.
func (d *bufferedErrorTopic) Add(m kafka.Message, qTypes ...ErrorQType) error {
	metadata := &DLQMetadata{
		RetryCount:  m.RetryCount() + 1,
		Data:        m.Key(),
		Topic:       m.Topic(),
		Partition:   m.Partition(),
		Offset:      m.Offset(),
		TimestampNs: m.Timestamp().UnixNano(),
	}

	key, err := proto.Marshal(metadata)
	if err != nil {
		d.logger.Error("failed to encode DLQ metadata", zap.Error(err))
		d.scope.Counter(metrics.KafkaDLQMetadataError).Inc(1)
		return err
	}
	value := m.Value()
	// TODO (gteo): Use a channel pool
	responseC := make(chan error)

	sm := d.newSaramaMessage(key, value, responseC)
	select {
	case d.producer.Input() <- sm:
	case <-d.stopC:
		return errShutdown
	}
	select {
	case err := <-responseC: // block until response is received
		if err == nil {
			d.scope.Counter(metrics.KafkaDLQMessagesOut).Inc(1)
		} else {
			d.scope.Counter(metrics.KafkaDLQErrors).Inc(1)
		}
		return err
	case <-d.stopC:
		return errShutdown
	}
}

func (d *bufferedErrorTopic) asyncProducerResponseLoop() {
	for {
		select {
		case msg := <-d.producer.Successes():
			if msg == nil {
				continue
			}
			responseC, ok := msg.Metadata.(chan error)
			if !ok {
				d.logger.Error("DLQ failed to decode metadata protobuf")
				continue
			}
			responseC <- nil
		case perr := <-d.producer.Errors():
			if perr == nil {
				continue
			}
			responseC, ok := perr.Msg.Metadata.(chan error)
			if !ok {
				continue
			}
			err := perr.Err
			responseC <- err
		case <-d.stopC:
			d.shutdown()
			return
		}
	}
}

func (d *bufferedErrorTopic) shutdown() {
	close(d.doneC)
}

func (d *bufferedErrorTopic) newSaramaMessage(key, value []byte, responseC chan error) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic:    d.topic.Name,
		Key:      sarama.ByteEncoder(key),
		Value:    sarama.ByteEncoder(value),
		Metadata: responseC,
	}
}

// NewRetryDLQMultiplexer returns a DLQ that will produce messages to retryTopic or dlqTopic depending on
// the threshold.
//
// Messages that are added to this DLQ will be sent to retryTopic if the retry count of the message is
// < the threshold.
// Else, it will go to the dlqTopic.
func NewRetryDLQMultiplexer(retryTopic, dlqTopic DLQ, threshold int64) DLQ {
	return &dlqMultiplexer{
		retryCountThreshold: threshold,
		retryTopic:          retryTopic,
		dlqTopic:            dlqTopic,
	}
}

// Add sends a kafka message to the retry topic or dlq topic depending on the retry count in the message.
//
// If the message RetryCount is greater than or equal to the retryCountThreshold in the multiplexer,
// the message will be sent to the retry topic.
// Else, it will be sent to the dlq topic.
func (d *dlqMultiplexer) Add(m kafka.Message, qtypes ...ErrorQType) error {
	// If qtypes is specified, use the first specified qtype as queue target
	if len(qtypes) > 0 {
		switch qtypes[0] {
		case DLQErrorQType:
			return d.dlqTopic.Add(m)
		default:
			return d.retryTopic.Add(m)
		}
	}

	// Queue type is not specified so use the retry count to determine queue target.
	if d.retryCountThreshold >= 0 && m.RetryCount() >= d.retryCountThreshold {
		return d.dlqTopic.Add(m)
	}
	return d.retryTopic.Add(m)
}

// Start retryDLQMultiplexer will start the retry and dlq buffered dlq producers.
func (d *dlqMultiplexer) Start() error {
	if err := d.retryTopic.Start(); err != nil {
		return err
	}
	if err := d.dlqTopic.Start(); err != nil {
		d.retryTopic.Stop()
		return err
	}
	return nil
}

// Stop closes the resources held by the retryTopic and dlqTopic.
func (d *dlqMultiplexer) Stop() {
	d.dlqTopic.Stop()
	d.retryTopic.Stop()
}

// NewNoopDLQ returns returns a noop DLQ.
func NewNoopDLQ() DLQ {
	return noopDLQ{}
}

// Start does nothing.
func (d noopDLQ) Start() error {
	return nil
}

// Stop does nothing.
func (d noopDLQ) Stop() {}

// Add returns errNoDLQ because there is no kafka topic backing it.
func (d noopDLQ) Add(m kafka.Message, qtypes ...ErrorQType) error {
	return errNoDLQ
}
