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
	"github.com/uber-go/kafka-client/internal/util"
	"github.com/uber-go/kafka-client/kafka"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var (
	errShutdown = errors.New("error consumer shutdown")
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
		Add(m kafka.Message) error
	}

	// bufferedErrorTopic is a client side abstraction for an error topic on the Kafka cluster.
	// This library uses the error topic as a base abstraction for retry and dlq topics.
	//
	// bufferedErrorTopic internally batches messages, so calls to Add may block until the internal
	// buffer is flushed and a batch of messages is send to the cluster.
	bufferedErrorTopic struct {
		topic    string
		cluster  string
		producer sarama.AsyncProducer

		stopC chan struct{}
		doneC chan struct{}

		scope     tally.Scope
		logger    *zap.Logger
		lifecycle *util.RunLifecycle
	}
)

// NewDLQ returns a DLQ writer for writing to a specific DLQ topic
func NewDLQ(topic, cluster string, client sarama.Client, scope tally.Scope, logger *zap.Logger) (DLQ, error) {
	asyncProducer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}
	return newBufferedDLQ(topic, cluster, asyncProducer, scope, logger), nil
}

func newBufferedDLQ(topic, cluster string, producer sarama.AsyncProducer, scope tally.Scope, logger *zap.Logger) *bufferedErrorTopic {
	return &bufferedErrorTopic{
		topic:     topic,
		cluster:   cluster,
		producer:  producer,
		stopC:     make(chan struct{}),
		doneC:     make(chan struct{}),
		scope:     scope,
		logger:    logger,
		lifecycle: util.NewRunLifecycle(fmt.Sprintf("dlqProducer-%s-%s", topic, cluster)),
	}
}

func (d *bufferedErrorTopic) Start() error {
	return d.lifecycle.Start(func() error {
		go d.asyncProducerResponseLoop()
		return nil
	})
}

func (d *bufferedErrorTopic) Stop() {
	d.lifecycle.Stop(func() {
		close(d.stopC)
		d.producer.Close()
		<-d.doneC
	})
}

func (d *bufferedErrorTopic) Add(m kafka.Message) error {
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
		// TODO (gteo): add metrics here
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
				d.logger.Error("fail to convert sarama ProducerMessage metadata to ")
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
		Topic:    d.topic,
		Key:      sarama.ByteEncoder(key),
		Value:    sarama.ByteEncoder(value),
		Metadata: responseC,
	}
}
