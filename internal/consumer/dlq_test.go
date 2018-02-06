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
	"github.com/Shopify/sarama"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type DLQTestSuite struct {
	suite.Suite
	saramaProducer *mockSaramaProducer
	dlq            *bufferedErrorTopic
}

func TestDLQTestSuite(t *testing.T) {
	suite.Run(t, new(DLQTestSuite))
}

func (s *DLQTestSuite) SetupTest() {
	s.saramaProducer = newMockSaramaProducer()
	s.dlq = newBufferedDLQ(
		"topic",
		"cluster",
		s.saramaProducer,
		tally.NoopScope,
		zap.NewNop(),
	)
}

func (s *DLQTestSuite) TestBatchProducerSuccessResponse() {
	m1 := &mockMessage{
		topic:     "topic",
		partition: 0,
		offset:    0,
	}

	s.dlq.Start()

	thread1 := make(chan error)
	go func() {
		err := s.dlq.Add(m1)
		thread1 <- err
	}()

	select {
	case <-thread1:
		s.Fail("add should block until response")
	case <-time.After(time.Millisecond):
		break
	}

	// flush success
	select {
	case pm := <-s.saramaProducer.inputC:
		s.saramaProducer.successC <- pm
	case <-time.After(time.Millisecond):
		s.Fail("")
	}

	select {
	case <-time.After(time.Millisecond):
		s.Fail("Expected thread1 to return nil")
	case err := <-thread1:
		s.NoError(err)
	}

	s.dlq.Stop()
}

func (s *DLQTestSuite) TestBatchProducerErrorResponse() {
	m1 := &mockMessage{
		topic:     "topic",
		partition: 0,
		offset:    0,
	}

	s.dlq.Start()

	thread1 := make(chan error)
	go func() {
		err := s.dlq.Add(m1)
		thread1 <- err
	}()

	select {
	case <-thread1:
		s.Fail("add should block until response")
	case <-time.After(time.Millisecond):
		break
	}

	// flush success
	select {
	case pm := <-s.saramaProducer.inputC:
		s.saramaProducer.errorC <- &sarama.ProducerError{Err: errors.New("error"), Msg: pm}
	case <-time.After(time.Millisecond):
		s.Fail("")
	}

	select {
	case <-time.After(time.Millisecond):
		s.Fail("Expected thread1 to return nil")
	case err := <-thread1:
		s.Error(err)
	}

	s.dlq.Stop()
}

func (s *DLQTestSuite) TestBatchProducerWaitingForResponseDoesNotDeadlock() {
	m1 := &mockMessage{
		topic:     "topic",
		partition: 0,
		offset:    0,
	}

	s.dlq.Start()

	thread1 := make(chan error)
	go func() {
		err := s.dlq.Add(m1)
		thread1 <- err
	}()

	select {
	case <-thread1:
		s.Fail("add should block until response or close")
	case <-time.After(time.Millisecond):
		break
	}

	s.dlq.Stop()

	select {
	case <-time.After(time.Millisecond):
		s.Fail("Expected thread1 to return nil")
	case err := <-thread1:
		s.Equal(errShutdown, err)
	}
}

func (s *DLQTestSuite) TestBatchProducerWaitingForProducerDoesNotDeadlock() {
	m1 := &mockMessage{
		topic:     "topic",
		partition: 0,
		offset:    0,
	}

	s.saramaProducer.inputC = make(chan *sarama.ProducerMessage)
	s.dlq.Start()

	thread1 := make(chan error)
	go func() {
		err := s.dlq.Add(m1)
		thread1 <- err
	}()

	select {
	case <-thread1:
		s.Fail("add should block until response or close")
	case <-time.After(time.Millisecond):
		break
	}

	s.dlq.Stop()

	select {
	case <-time.After(time.Millisecond):
		s.Fail("Expected thread1 to return nil")
	case err := <-thread1:
		s.Equal(errShutdown, err)
	}
}

type DLQMultiplexerTestSuite struct {
	suite.Suite
	msg         *mockMessage
	retry       *mockDLQProducer
	dlq         *mockDLQProducer
	multiplexer *retryDLQMultiplexer
}

func TestDLQMultiplexerTestSuite(t *testing.T) {
	suite.Run(t, new(DLQMultiplexerTestSuite))
}

func (s *DLQMultiplexerTestSuite) SetupTest() {
	s.msg = new(mockMessage)
	s.retry = newMockDLQProducer()
	s.dlq = newMockDLQProducer()
	s.multiplexer = &retryDLQMultiplexer{
		retryCountThreshold: 3,
		retryTopic:          s.retry,
		dlqTopic:            s.dlq,
	}
}

func (s *DLQMultiplexerTestSuite) TestAdd() {
	s.multiplexer.Add(s.msg)
	s.Equal(1, s.retry.backlog())
	s.Equal(0, s.dlq.backlog())

	s.msg.retryCount = 3
	s.multiplexer.Add(s.msg)
	s.Equal(1, s.dlq.backlog())
}
