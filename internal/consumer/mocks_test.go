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
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/gig/kafka-client/internal/util"
	"github.com/gig/kafka-client/kafka"
	"go.uber.org/zap/zapcore"
)

type (
	mockConsumer struct {
		sync.Mutex
		name      string
		topics    []string
		startErr  error
		msgC      chan kafka.Message
		doneC     chan struct{}
		lifecycle *util.RunLifecycle
	}
	mockMessage struct {
		key        []byte
		value      []byte
		topic      string
		partition  int32
		offset     int64
		timestamp  time.Time
		retryCount int64
		ackErr     error
		nackErr    error
	}

	mockSaramaClient struct {
		closed int32
	}
	mockSaramaProducer struct {
		closed   int32
		inputC   chan *sarama.ProducerMessage
		successC chan *sarama.ProducerMessage
		errorC   chan *sarama.ProducerError
	}
	mockSaramaConsumer struct {
		sync.Mutex
		closed     int64
		offsets    map[int32]int64
		errorC     chan error
		notifyC    chan *cluster.Notification
		partitionC chan cluster.PartitionConsumer
		messages   chan *sarama.ConsumerMessage
	}
	mockPartitionedConsumer struct {
		id          int32
		topic       string
		closed      int64
		beginOffset int64
		msgC        chan *sarama.ConsumerMessage
	}
	mockDLQProducer struct {
		sync.Mutex
		stopped  int32
		messages []kafka.Message
	}
)

func newMockSaramaProducer() *mockSaramaProducer {
	return &mockSaramaProducer{
		inputC:   make(chan *sarama.ProducerMessage, 10),
		successC: make(chan *sarama.ProducerMessage, 10),
		errorC:   make(chan *sarama.ProducerError, 10),
	}
}

func (m *mockSaramaProducer) AsyncClose() {
	atomic.AddInt32(&m.closed, 1)
}

func (m *mockSaramaProducer) Close() error {
	m.AsyncClose()
	return nil
}

func (m *mockSaramaProducer) Input() chan<- *sarama.ProducerMessage {
	return m.inputC
}

func (m *mockSaramaProducer) Successes() <-chan *sarama.ProducerMessage {
	return m.successC
}

func (m *mockSaramaProducer) Errors() <-chan *sarama.ProducerError {
	return m.errorC
}

func newMockConsumer(name string, topics []string, msgC chan kafka.Message) *mockConsumer {
	return &mockConsumer{
		name:      name,
		topics:    topics,
		msgC:      msgC,
		doneC:     make(chan struct{}),
		lifecycle: util.NewRunLifecycle("mockConsumer-" + name),
	}
}

// Name returns the id for this mockConsumer.
func (c *mockConsumer) Name() string {
	c.Lock()
	defer c.Unlock()
	return c.name
}

// Topics returns the list of topics this mock consumer was assigned to consumer.
func (c *mockConsumer) Topics() []string {
	c.Lock()
	defer c.Unlock()
	return c.topics
}

// Start will start the mockConsumer on the initialized lifecycle and return startErr.
func (c *mockConsumer) Start() error {
	return c.lifecycle.Start(func() error {
		return c.startErr
	})
}

// Stop will stop the lifecycle.
func (c *mockConsumer) Stop() {
	c.lifecycle.Stop(func() {
		close(c.doneC)
	})
}

// Closed will return a channel that can be used to check if the consumer has been stopped.
func (c *mockConsumer) Closed() <-chan struct{} {
	return c.doneC
}

// Messages return the message channel.
func (c *mockConsumer) Messages() <-chan kafka.Message {
	return c.msgC
}

func (m *mockMessage) MarshalLogObject(zapcore.ObjectEncoder) error {
	return nil
}

func (m *mockMessage) Key() []byte {
	return m.key
}

func (m *mockMessage) Value() []byte {
	return m.value
}

func (m *mockMessage) Topic() string {
	return m.topic
}

func (m *mockMessage) Partition() int32 {
	return m.partition
}

func (m *mockMessage) Offset() int64 {
	return m.offset
}

func (m *mockMessage) Timestamp() time.Time {
	return m.timestamp
}

func (m *mockMessage) RetryCount() int64 {
	return m.retryCount
}

func (m *mockMessage) Ack() error {
	return m.ackErr
}

func (m *mockMessage) Nack() error {
	return m.nackErr
}

func (m *mockMessage) NackToDLQ() error {
	return m.nackErr
}

func newMockPartitionedConsumer(topic string, id int32, beginOffset int64, rcvBufSize int) *mockPartitionedConsumer {
	return &mockPartitionedConsumer{
		id:          id,
		topic:       topic,
		beginOffset: beginOffset,
		msgC:        make(chan *sarama.ConsumerMessage, rcvBufSize),
	}
}

func (m *mockPartitionedConsumer) start() *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		offset := m.beginOffset + 1
		for i := 0; i < 100; i++ {
			m.sendMsg(offset)
			offset++
		}
		wg.Done()
	}()
	return &wg
}

func (m *mockPartitionedConsumer) stop() {
	close(m.msgC)
}

func (m *mockPartitionedConsumer) sendMsg(offset int64) {
	msg := &sarama.ConsumerMessage{
		Topic:     m.topic,
		Partition: m.id,
		Value:     []byte(fmt.Sprintf("msg-%v", offset)),
		Offset:    offset,
		Timestamp: time.Now(),
	}
	m.msgC <- msg
}

func (m *mockPartitionedConsumer) Close() error {
	atomic.StoreInt64(&m.closed, 1)
	return nil
}

func (m *mockPartitionedConsumer) isClosed() bool {
	return atomic.LoadInt64(&m.closed) == 1
}

func (m *mockPartitionedConsumer) AsyncClose() {}

func (m *mockPartitionedConsumer) Errors() <-chan *sarama.ConsumerError {
	return nil
}

// Messages returns the read channel for the messages that are returned by
// the broker.
func (m *mockPartitionedConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return m.msgC
}

// HighWaterMarkOffset returns the high water mark offset of the partition,
// i.e. the offset that will be used for the next message that will be produced.
// You can use this to determine how far behind the processing is.
func (m *mockPartitionedConsumer) HighWaterMarkOffset() int64 {
	return 0
}

// Topic returns the consumed topic name
func (m *mockPartitionedConsumer) Topic() string {
	return m.topic
}

// Partition returns the consumed partition
func (m *mockPartitionedConsumer) Partition() int32 {
	return m.id
}

func newMockSaramaConsumer() *mockSaramaConsumer {
	return &mockSaramaConsumer{
		errorC:     make(chan error, 1),
		notifyC:    make(chan *cluster.Notification, 1),
		partitionC: make(chan cluster.PartitionConsumer, 1),
		offsets:    make(map[int32]int64),
		messages:   make(chan *sarama.ConsumerMessage, 1),
	}
}

func (m *mockSaramaConsumer) offset(id int) int64 {
	m.Lock()
	off, ok := m.offsets[int32(id)]
	m.Unlock()
	if !ok {
		return 0
	}
	return off
}

func (m *mockSaramaConsumer) ResetPartitionOffset(topic string, partition int32, offset int64, metadata string) {
}

func (m *mockSaramaConsumer) Errors() <-chan error {
	return m.errorC
}

func (m *mockSaramaConsumer) Notifications() <-chan *cluster.Notification {
	return m.notifyC
}

func (m *mockSaramaConsumer) Partitions() <-chan cluster.PartitionConsumer {
	return m.partitionC
}

func (m *mockSaramaConsumer) CommitOffsets() error {
	return nil
}

func (m *mockSaramaConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return m.messages
}

func (m *mockSaramaConsumer) MarkOffset(msg *sarama.ConsumerMessage, metadata string) {
	m.Lock()
	m.offsets[msg.Partition] = msg.Offset
	m.Unlock()
}

func (m *mockSaramaConsumer) MarkPartitionOffset(topic string, partition int32, offset int64, metadata string) {
	m.Lock()
	m.offsets[partition] = offset
	m.Unlock()
}

func (m *mockSaramaConsumer) HighWaterMarks() map[string]map[int32]int64 {
	result := make(map[string]map[int32]int64)
	result["test"] = make(map[int32]int64)
	m.Lock()
	for k, v := range m.offsets {
		result["test"][k] = v
	}
	m.Unlock()
	return result
}

func (m *mockSaramaConsumer) Close() error {
	atomic.AddInt64(&m.closed, 1)
	return nil
}

func (m *mockSaramaConsumer) isClosed() bool {
	return atomic.LoadInt64(&m.closed) == 1
}

func newMockDLQProducer() *mockDLQProducer {
	return &mockDLQProducer{
		messages: make([]kafka.Message, 0, 100),
		stopped:  0,
	}
}

func (d *mockDLQProducer) Start() error {
	return nil
}

func (d *mockDLQProducer) Stop() {
	atomic.AddInt32(&d.stopped, 1)
}

func (d *mockDLQProducer) Add(m kafka.Message, qType ...ErrorQType) error {
	d.Lock()
	defer d.Unlock()
	d.messages = append(d.messages, m)
	return nil
}

func (d *mockDLQProducer) isClosed() bool {
	return atomic.LoadInt32(&d.stopped) > 0
}
func (d *mockDLQProducer) backlog() int {
	d.Lock()
	defer d.Unlock()
	return len(d.messages)
}

func newMockSaramaClient() *mockSaramaClient {
	return &mockSaramaClient{
		closed: 0,
	}
}

func (m *mockSaramaClient) Config() *sarama.Config {
	panic("implement me")
}

func (m *mockSaramaClient) Brokers() []*sarama.Broker {
	panic("implement me")
}

func (m *mockSaramaClient) Controller() (*sarama.Broker, error) {
	panic("implement me")
}

func (m *mockSaramaClient) Topics() ([]string, error) {
	panic("implement me")
}

func (m *mockSaramaClient) Partitions(topic string) ([]int32, error) {
	panic("implement me")
}

func (m *mockSaramaClient) WritablePartitions(topic string) ([]int32, error) {
	panic("implement me")
}

func (m *mockSaramaClient) Leader(topic string, partitionID int32) (*sarama.Broker, error) {
	panic("implement me")
}

func (m *mockSaramaClient) Replicas(topic string, partitionID int32) ([]int32, error) {
	panic("implement me")
}

func (m *mockSaramaClient) InSyncReplicas(topic string, partitionID int32) ([]int32, error) {
	panic("implement me")
}

func (m *mockSaramaClient) RefreshMetadata(topics ...string) error {
	panic("implement me")
}

func (m *mockSaramaClient) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	panic("implement me")
}

func (m *mockSaramaClient) Coordinator(consumerGroup string) (*sarama.Broker, error) {
	panic("implement me")
}

func (m *mockSaramaClient) RefreshCoordinator(consumerGroup string) error {
	panic("implement me")
}

func (m *mockSaramaClient) Close() error {
	atomic.AddInt32(&m.closed, 1)
	return nil
}

func (m *mockSaramaClient) Closed() bool {
	return atomic.LoadInt32(&m.closed) > 0
}
