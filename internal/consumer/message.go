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
	"github.com/Shopify/sarama"
	"github.com/uber-go/kafka-client/kafkacore"
	"time"
)

type (
	// Message is a wrapper around kafka consumer message
	Message struct {
		kafkacore.Message
		ctx msgContext // consumer metadata, invisible to the application
	}

	// kafkacoreMessage is an implementation of kafkacore.Message
	kafkacoreMessage struct {
		msg *sarama.ConsumerMessage
	}

	// context that gets piggybacked in the message
	// will be used when the message is Acked/Nackd
	msgContext struct {
		offset int64
		ackID  ackID
		ackMgr *ackManager
		dlq    DLQ
	}
)

func newMessage(msg kafkacore.Message, ctx msgContext) *Message {
	return &Message{
		Message: msg,
		ctx:     ctx,
	}
}

// Key is a mutable reference to the message's key
func (m *kafkacoreMessage) Key() []byte {
	result := make([]byte, len(m.msg.Key))
	copy(result, m.msg.Key)
	return result
}

// Value is a mutable reference to the message's value
func (m *kafkacoreMessage) Value() []byte {
	result := make([]byte, len(m.msg.Value))
	copy(result, m.msg.Value)
	return result
}

// Topic is the topic from which the message was read
func (m *kafkacoreMessage) Topic() string {
	return m.msg.Topic
}

// Partition is the ID of the partition from which the message was read
func (m *kafkacoreMessage) Partition() int32 {
	return m.msg.Partition
}

// Offset is the message's offset.
func (m *kafkacoreMessage) Offset() int64 {
	return m.msg.Offset
}

// Timestamp returns the timestamp for this message
func (m *kafkacoreMessage) Timestamp() time.Time {
	return m.msg.Timestamp
}

// Ack acknowledges the message
func (m *Message) Ack() error {
	ctx := &m.ctx
	ctx.ackMgr.Ack(ctx.ackID)
	return nil
}

// Nack negatively acknowledges the message
// also moves the message to a DLQ if the
// consumer has a dlq configured. This method
// will *block* until enqueue to the dlq succeeds
func (m *Message) Nack() error {
	ctx := &m.ctx
	ctx.dlq.Add(m)
	ctx.ackMgr.Nack(ctx.ackID)
	return nil
}
