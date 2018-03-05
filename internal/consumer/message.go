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
	"go.uber.org/zap/zapcore"
)

type (
	// Message is a wrapper around kafka consumer message
	Message struct {
		msg      *sarama.ConsumerMessage
		metadata DLQMetadata
		ctx      msgContext // consumer metadata, invisible to the application
	}
	// context that gets piggybacked in the message
	// will be used when the message is Acked/Nackd
	msgContext struct {
		ackID  ackID
		ackMgr *ackManager
		dlq    DLQ
	}
)

func newDLQMetadata() *DLQMetadata {
	return &DLQMetadata{
		RetryCount:  0,
		Topic:       "",
		Partition:   -1,
		Offset:      -1,
		TimestampNs: -1,
		Data:        nil,
	}
}

// newMessage builds a new Message object from the given kafka message
func newMessage(scm *sarama.ConsumerMessage, ackID ackID, ackMgr *ackManager, dlq DLQ, metadata DLQMetadata) *Message {
	return &Message{
		msg: scm,
		ctx: msgContext{
			ackID:  ackID,
			ackMgr: ackMgr,
			dlq:    dlq,
		},
		metadata: metadata,
	}
}

// Key is a mutable reference to the message's key
func (m *Message) Key() (key []byte) {
	if m.metadata.Data != nil {
		key = make([]byte, len(m.metadata.Data))
		copy(key, m.metadata.Data)
	} else {
		key = make([]byte, len(m.msg.Key))
		copy(key, m.msg.Key)
	}
	return
}

// Value is a mutable reference to the message's value
func (m *Message) Value() []byte {
	result := make([]byte, len(m.msg.Value))
	copy(result, m.msg.Value)
	return result
}

// Topic is the topic from which the message was read
func (m *Message) Topic() string {
	if m.metadata.Topic != "" {
		return m.metadata.Topic
	}
	return m.msg.Topic
}

// Partition is the ID of the partition from which the message was read
func (m *Message) Partition() int32 {
	if m.metadata.Partition != -1 {
		return m.metadata.Partition
	}
	return m.msg.Partition
}

// Offset is the message's offset.
func (m *Message) Offset() int64 {
	if m.metadata.Offset != -1 {
		return m.metadata.Offset
	}
	return m.msg.Offset
}

// Timestamp returns the timestamp for this message
func (m *Message) Timestamp() time.Time {
	if m.metadata.TimestampNs != -1 {
		return time.Unix(0, m.metadata.TimestampNs)
	}
	return m.msg.Timestamp
}

// RetryCount returns the number of times this message has be retried.
func (m *Message) RetryCount() int64 {
	return m.metadata.RetryCount
}

// Ack acknowledges the message
func (m *Message) Ack() error {
	ctx := &m.ctx
	return ctx.ackMgr.Ack(ctx.ackID)
}

// Nack negatively acknowledges the message
// also moves the message to a DLQ if the
// consumer has a dlq configured. This method
// will *block* until enqueue to the dlq succeeds
func (m *Message) Nack() error {
	ctx := &m.ctx
	if err := ctx.dlq.Add(m); err != nil {
		return err
	}
	return ctx.ackMgr.Nack(ctx.ackID)
}

// MarshalLogObject implements zapcore.ObjectMarshaler for structured logging.
func (m *Message) MarshalLogObject(e zapcore.ObjectEncoder) error {
	e.AddString("topic", m.Topic())
	e.AddInt32("partition", m.Partition())
	e.AddInt64("offset", m.Offset())
	e.AddTime("timestamp", m.Timestamp())
	e.AddInt64("retryCount", m.RetryCount())

	if m.metadata.RetryCount != 0 {
		e.AddObject("underlyingMsg", zapcore.ObjectMarshalerFunc(func(ee zapcore.ObjectEncoder) error {
			ee.AddString("topic", m.msg.Topic)
			ee.AddInt32("partition", m.msg.Partition)
			ee.AddInt64("offset", m.msg.Offset)
			ee.AddTime("timestamp", m.msg.Timestamp)
			return nil
		}))
	}
	return nil
}
