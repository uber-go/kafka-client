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

	"github.com/stretchr/testify/suite"
	"github.com/uber-go/kafka-client/kafka"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const (
	testWaitDuration = 10 * time.Millisecond
)

type RangePartitionConsumerTestSuite struct {
	suite.Suite
	msgC                    chan kafka.Message
	partitionConsumer       *partitionConsumer
	rangePartitionConsumer  *rangePartitionConsumer
	saramaConsumer          *mockSaramaConsumer
	saramaPartitionConsumer *mockPartitionedConsumer
}

func TestRangePartitionConsumerTestSuite(t *testing.T) {
	suite.Run(t, new(RangePartitionConsumerTestSuite))
}

func (s *RangePartitionConsumerTestSuite) SetupTest() {
	s.msgC = make(chan kafka.Message, 5)
	s.saramaConsumer = newMockSaramaConsumer()
	s.saramaPartitionConsumer = newMockPartitionedConsumer("topic", 0, 100, 5)
	topic := new(Topic)
	topic.Name = "topic"
	topic.TopicType = TopicTypeDefaultQ
	opts := DefaultOptions()
	l := zap.NewNop()
	s.partitionConsumer = newPartitionConsumer(
		*topic,
		s.saramaConsumer,
		s.saramaPartitionConsumer,
		opts,
		s.msgC,
		nil,
		tally.NoopScope,
		l,
	)
	s.rangePartitionConsumer = newRangePartitionConsumer(s.partitionConsumer)
}

func (s *RangePartitionConsumerTestSuite) TestOffsetNotificationTriggersMessageConsuming() {
	s.rangePartitionConsumer.Start()

	s.saramaPartitionConsumer.sendMsg(100) // this should be consumed
	s.saramaPartitionConsumer.sendMsg(101) // this message should be ignored
	s.saramaPartitionConsumer.sendMsg(91)  // this should be the first message after ResetOffset
	s.saramaPartitionConsumer.sendMsg(92)  // this should be second message after ResetOffset
	s.saramaPartitionConsumer.sendMsg(93)  // this message should be ignored b/c it is larger than HighOffset of OffsetRange

	// set offset range to 100-100, so receive one message
	s.rangePartitionConsumer.ResetOffset(kafka.OffsetRange{LowOffset: 100, HighOffset: 100})
	select {
	case msg := <-s.msgC:
		s.EqualValues(100, msg.Offset())
		msg.Ack()
		break
	case <-time.After(testWaitDuration):
		s.Fail("expected message 100")
	}

	// ackmgr should be at 100 since we commit 100
	s.EqualValues(100, s.rangePartitionConsumer.ackMgr.CommitLevel())

	// Trigger reset offset
	s.rangePartitionConsumer.ResetOffset(kafka.OffsetRange{LowOffset: 91, HighOffset: 92})
	for i := 0; i < 2; i++ {
		select {
		case msg := <-s.msgC:
			s.EqualValues(91+i, msg.Offset())
			msg.Ack()
		case <-time.After(testWaitDuration):
			s.Fail("expected 2 messages on msgC")
		}
	}

	select {
	case <-s.msgC:
		s.Fail("expect only 2 messages on msgC")
	case <-time.After(testWaitDuration):
		break
	}

	// ackMgr should reset to 90
	s.EqualValues(92, s.rangePartitionConsumer.ackMgr.CommitLevel())
	s.rangePartitionConsumer.Stop()
}
