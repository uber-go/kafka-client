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
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"github.com/uber-go/kafka-client/internal/backoff"
	"github.com/uber-go/kafka-client/internal/util"
	"github.com/uber-go/kafka-client/kafka"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
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

	bufferedDLQImpl struct {
		topic    string
		producer sarama.SyncProducer

		timeLimit <-chan time.Time
		sizeLimit <-chan time.Time
		stopC     chan struct{}
		closedC   chan struct{}

		messageBufferMap *messageBufferMap
		errorMap         *errorMap

		tally     tally.Scope
		logger    *zap.Logger
		lifecycle *util.RunLifecycle
	}

	errorMap struct {
		m    map[string]error
		cond *sync.Cond
	}

	messageBufferMap struct {
		buf map[string]*sarama.ProducerMessage
		sync.Mutex
	}
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

func (d *dlqImpl) Start() error {
	return nil
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

func (d *dlqImpl) Stop() {
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
	metadata := &DLQMetadata{
		RetryCount:  0,
		Data:        m.Key(),
		Topic:       m.Topic(),
		Partition:   m.Partition(),
		Offset:      m.Offset(),
		TimestampNs: m.Timestamp().UnixNano(),
	}
	return proto.Marshal(metadata)
}

// NewDLQNoop returns a DLQ that drops everything on the floor
// this is used only when DLQ is not configured for a consumer
func NewDLQNoop() DLQ {
	return &dlqNoop{}
}
func (d *dlqNoop) Add(m kafka.Message) error { return nil }
func (d *dlqNoop) Stop()                     {}
func (d *dlqNoop) Start() error              { return nil }

func newBufferedDLQImpl(topic string, producer sarama.SyncProducer, timeLimit <-chan time.Time, sizeLimit <-chan time.Time, tally tally.Scope, log *zap.Logger) *bufferedDLQImpl {
	return &bufferedDLQImpl{
		topic:            topic,
		producer:         producer,
		timeLimit:        timeLimit,
		sizeLimit:        sizeLimit,
		stopC:            make(chan struct{}),
		closedC:          make(chan struct{}),
		messageBufferMap: newMessageBufferMap(),
		errorMap:         newErrorMap(),
		tally:            tally,
		logger:           log,
		lifecycle:        util.NewRunLifecycle(fmt.Sprintf("dlq-%s", topic), log),
	}
}

func (d *bufferedDLQImpl) Start() error {
	return d.lifecycle.Start(func() error {
		go d.eventLoop()
		return nil
	})
}

func (d *bufferedDLQImpl) Stop() {
	d.lifecycle.Stop(func() {
		close(d.stopC)
		<-d.closedC
	})
}

func (d *bufferedDLQImpl) Add(msg kafka.Message) error {
	key := kafkaMessageKey(msg)
	sm := d.newSaramaMessage(msg)
	d.messageBufferMap.Add(key, sm)
	return d.errorMap.GetAndRemove(key) // errorMap.Get blocks until there is error or nil for this message
}

func (d *bufferedDLQImpl) eventLoop() {
	for {
		select {
		case <-d.timeLimit:
			d.flush()
		case <-d.sizeLimit:
			d.flush()
		case <-d.stopC:
			d.shutdown()
			return
		}
	}
}

func (d *bufferedDLQImpl) shutdown() {
	d.flush()
	close(d.closedC)
}

func (d *bufferedDLQImpl) flush() {
	d.messageBufferMap.Lock()
	defer d.messageBufferMap.Unlock()
	defer d.messageBufferMap.clear()

	// Get and send buffered messages
	err := d.producer.SendMessages(d.messageBufferMap.messages())

	// Some messages may fail, so parse failures into errorMap
	producerErrs, ok := err.(sarama.ProducerErrors)
	if !ok {
		d.errorMap.PutAll(d.messageBufferMap.keys(), fmt.Errorf("failed to send message to DLQ"))
		return
	}

	for _, producerErr := range producerErrs {
		err := producerErr.Err
		key, ok := producerErr.Msg.Metadata.(string)
		if !ok {
			d.errorMap.Put(key, fmt.Errorf("failed to send message to DLQ"))
			continue
		}
		d.errorMap.Put(key, err)
	}

	// Put nil as default for the errors in errorMap
	// because cond waits on non-existence of key.
	d.errorMap.PutAll(d.messageBufferMap.keys(), nil)

	// Broadcast that errorMap has been updated
	d.errorMap.cond.Broadcast()
}

func kafkaMessageKey(msg kafka.Message) string {
	return fmt.Sprintf("%s%d%d", msg.Topic(), msg.Partition(), msg.Offset())
}

func (d *bufferedDLQImpl) newSaramaMessage(m kafka.Message) *sarama.ProducerMessage {
	return &sarama.ProducerMessage{
		Topic:    d.topic,
		Key:      sarama.StringEncoder(m.Key()),
		Value:    sarama.StringEncoder(m.Value()),
		Metadata: kafkaMessageKey(m),
	}
}

func newErrorMap() *errorMap {
	l := new(sync.Mutex)
	return &errorMap{
		m:    make(map[string]error),
		cond: sync.NewCond(l),
	}
}

// GetAndRemove returns the error or nil associated with this key.
func (m *errorMap) GetAndRemove(key string) error {
	m.cond.L.Lock()
	for !m.contains(key) {
		m.cond.Wait()
	}
	err, _ := m.m[key]
	delete(m.m, key)
	m.cond.L.Unlock()
	return err
}

func (m *errorMap) Put(key string, err error) {
	m.cond.L.Lock()
	m.m[key] = err
	m.cond.L.Unlock()
}

// PutAll assigns the err to all of the keys specified in the input unless the key already exists.
// If the key already exists, then the old value is retained.
func (m *errorMap) PutAll(keys []string, err error) {
	m.cond.L.Lock()
	for _, key := range keys {
		if _, ok := m.m[key]; !ok {
			m.m[key] = err
		}
	}
	m.cond.L.Unlock()
}

func (m *errorMap) contains(key string) bool {
	_, ok := m.m[key]
	return ok
}

func (m *errorMap) Size() int {
	m.cond.L.Lock()
	defer m.cond.L.Unlock()
	return len(m.m)
}

func newMessageBufferMap() *messageBufferMap {
	return &messageBufferMap{
		buf: make(map[string]*sarama.ProducerMessage),
	}
}

func (m *messageBufferMap) Add(key string, msg *sarama.ProducerMessage) {
	m.Lock()
	m.buf[key] = msg
	m.Unlock()
}

func (m *messageBufferMap) clear() {
	for key := range m.buf {
		delete(m.buf, key)
	}
}

func (m *messageBufferMap) messages() []*sarama.ProducerMessage {
	buf := make([]*sarama.ProducerMessage, 0, len(m.buf))
	for _, msg := range m.buf {
		buf = append(buf, msg)
	}
	return buf
}

func (m *messageBufferMap) keys() []string {
	buf := make([]string, 0, len(m.buf))
	for key := range m.buf {
		buf = append(buf, key)
	}
	return buf
}

func (m *messageBufferMap) Size() int {
	m.Lock()
	defer m.Unlock()
	return len(m.buf)
}
