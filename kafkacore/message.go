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

package kafkacore

import (
	"time"
)

type (
	// MessageTransformer is an interface that the library will use to transform a message.
	// You can use this to change the default implementation of message accessors methods.
	MessageTransformer interface {
		Transform(msg Message) Message
	}

	// Message is the interface that exposes the Message contents useful for the end user.
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
	}

	noopMessageTransformer struct{}
)

// NewNoopMessageTransformer returns a new MessageTransformer that does not transform the message at all.
func NewNoopMessageTransformer() MessageTransformer {
	return noopMessageTransformer{}
}

// Transform simply pass the incoming message through unchanged.
func (m noopMessageTransformer) Transform(msg Message) Message {
	return msg
}
