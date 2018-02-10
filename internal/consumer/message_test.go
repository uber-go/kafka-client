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
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/kafka-client/kafka"
)

type MessageTestSuite struct {
	suite.Suite
	message    *Message
	dlqMessage *Message
}

var _ kafka.Message = (*Message)(nil)

func TestMessageTestSuite(t *testing.T) {
	suite.Run(t, new(MessageTestSuite))
}

func (s *MessageTestSuite) SetupTest() {
	saramaMessage := &sarama.ConsumerMessage{
		Key:       []byte("key1"),
		Value:     []byte("value1"),
		Topic:     "topic1",
		Partition: 1,
		Offset:    1,
		Timestamp: time.Unix(1, 0),
	}
	metadata := &DLQMetadata{
		RetryCount:  1,
		Topic:       "topic2",
		Partition:   2,
		Offset:      2,
		TimestampNs: 100,
		Data:        []byte("key2"),
	}
	ackID := new(ackID)
	s.message = newMessage(saramaMessage, *ackID, nil, nil, *newDLQMetadata())
	s.dlqMessage = newMessage(saramaMessage, *ackID, nil, nil, *metadata)
}

func (s *MessageTestSuite) TestKey() {
	s.EqualValues(s.message.msg.Key, s.message.Key())
	s.EqualValues(s.dlqMessage.metadata.Data, s.dlqMessage.Key())
}

func (s *MessageTestSuite) TestValue() {
	s.EqualValues(s.message.msg.Value, s.message.Value())
	s.EqualValues(s.dlqMessage.msg.Value, s.dlqMessage.Value())
}

func (s *MessageTestSuite) TestTopic() {
	s.EqualValues(s.message.msg.Topic, s.message.Topic())
	s.EqualValues(s.dlqMessage.metadata.Topic, s.dlqMessage.Topic())
}

func (s *MessageTestSuite) TestPartition() {
	s.EqualValues(s.message.msg.Partition, s.message.Partition())
	s.EqualValues(s.dlqMessage.metadata.Partition, s.dlqMessage.Partition())
}

func (s *MessageTestSuite) TestOffset() {
	s.EqualValues(s.message.msg.Offset, s.message.Offset())
	s.EqualValues(s.dlqMessage.metadata.Offset, s.dlqMessage.Offset())
}

func (s *MessageTestSuite) TestTimestamp() {
	s.EqualValues(s.message.msg.Timestamp, s.message.Timestamp())
	s.EqualValues(time.Unix(0, s.dlqMessage.metadata.TimestampNs), s.dlqMessage.Timestamp())
}

func (s *MessageTestSuite) TestRetryCount() {
	s.EqualValues(s.message.metadata.RetryCount, s.message.RetryCount())
	s.EqualValues(s.dlqMessage.metadata.RetryCount, s.dlqMessage.RetryCount())
}
