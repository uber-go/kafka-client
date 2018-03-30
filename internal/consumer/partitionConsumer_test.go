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
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const (
	testWaitDuration = 2 * time.Second
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
	topic.DLQMetadataDecoder = NoopDLQMetadataDecoder
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

func (s *RangePartitionConsumerTestSuite) TestDelayMsg() {
	m := &sarama.ConsumerMessage{
		Offset:    100,
		Timestamp: time.Now(),
	}
	t1 := time.Now()
	delay := time.Millisecond
	s.rangePartitionConsumer.topicPartition.Delay = delay
	s.rangePartitionConsumer.delayMsg(m)
	t2 := time.Now()
	if t2.Sub(t1) < delay {
		s.Fail("expect to sleep around " + delay.String())
	}

	// lag > delay, almost return immediately
	t1 = time.Now()
	m.Timestamp = t1.Add(time.Millisecond * -3)
	s.rangePartitionConsumer.delayMsg(m)
	t2 = time.Now()
	if t2.Sub(t1) > time.Millisecond {
		s.Fail("expect no delay on msg, actual time cost is " + t2.Sub(t1).String())
	}

	// delay = 0, almost return immediately
	t1 = time.Now()
	s.rangePartitionConsumer.topicPartition.Delay = 0
	m.Timestamp = t1.Add(time.Millisecond)
	s.rangePartitionConsumer.delayMsg(m)
	t2 = time.Now()
	if t2.Sub(t1) > time.Millisecond {
		s.Fail("expect no delay on msg")
	}
}

func (s *RangePartitionConsumerTestSuite) TestOffsetNotificationTriggersMessageConsuming() {
	s.rangePartitionConsumer.Start()

	s.saramaPartitionConsumer.sendMsg(100) // this should be consumed
	s.saramaPartitionConsumer.sendMsg(101) // this message should be ignored
	s.saramaPartitionConsumer.sendMsg(91)  // this should be the first message after ResetOffset
	s.saramaPartitionConsumer.sendMsg(92)  // this should be second message after ResetOffset
	s.saramaPartitionConsumer.sendMsg(93)  // this message should be ignored b/c it is larger than HighOffset of OffsetRange

	// set offset range to 100-100, so receive one message
	s.NoError(s.rangePartitionConsumer.ResetOffset(kafka.OffsetRange{LowOffset: 100, HighOffset: 100}))
	select {
	case msg := <-s.msgC:
		s.EqualValues(100, msg.Offset())
		msg.Ack()
	case <-time.After(testWaitDuration):
		s.Fail("expected message 100")
	}

	// ackmgr should be at 100 since we commit 100
	s.EqualValues(100, s.rangePartitionConsumer.ackMgr.CommitLevel())

	// Trigger reset offset
	s.NoError(s.rangePartitionConsumer.ResetOffset(kafka.OffsetRange{LowOffset: 91, HighOffset: 92}))
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

func (s *RangePartitionConsumerTestSuite) TestResetOffsetNoHighOffsetReturnsError() {
	s.Error(s.rangePartitionConsumer.ResetOffset(kafka.OffsetRange{LowOffset: 90, HighOffset: -1}))
}

func (s *RangePartitionConsumerTestSuite) TestResetOffsetStoppedReturnsError() {
	close(s.rangePartitionConsumer.stopC)
	s.Error(s.rangePartitionConsumer.ResetOffset(kafka.OffsetRange{LowOffset: 90, HighOffset: 100}))
}

func (s *RangePartitionConsumerTestSuite) TestResetOffsetWithCurrentOffsetReturnsNil() {
	s.rangePartitionConsumer.offsetRangeLock.Lock()
	s.rangePartitionConsumer.offsetRange = &kafka.OffsetRange{LowOffset: 90, HighOffset: 100}
	s.rangePartitionConsumer.offsetRangeLock.Unlock()
	s.NoError(s.rangePartitionConsumer.ResetOffset(kafka.OffsetRange{LowOffset: 95, HighOffset: 100}))
}
