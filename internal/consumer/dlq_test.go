package consumer

import (
	"fmt"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/kafka-client/internal/util"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type DLQTestSuite struct {
	suite.Suite
	saramaProducer *mockSaramaProducer
	dlq            *bufferedDLQImpl
	timeLimit      chan time.Time
	sizeLimit      chan time.Time
}

func TestDLQTestSuite(t *testing.T) {
	suite.Run(t, new(DLQTestSuite))
}

func (s *DLQTestSuite) SetupTest() {
	s.timeLimit = make(chan time.Time)
	s.sizeLimit = make(chan time.Time)
	s.saramaProducer = newMockSaramaProducer()
	logger := zap.NewNop()
	s.dlq = &bufferedDLQImpl{
		topic:            "topic",
		producer:         s.saramaProducer,
		timeLimit:        s.timeLimit,
		sizeLimit:        s.sizeLimit,
		stopC:            make(chan struct{}),
		closedC:          make(chan struct{}),
		messageBufferMap: newMessageBufferMap(),
		errorMap:         newErrorMap(),
		tally:            tally.NoopScope,
		logger:           logger,
		lifecycle:        util.NewRunLifecycle("dlqTest", logger),
	}
}

func (s *DLQTestSuite) TestBatchProducer() {
	m1 := &mockMessage{
		topic:     "topic",
		partition: 0,
		offset:    0,
	}
	m2 := &mockMessage{
		topic:     "topic",
		partition: 0,
		offset:    1,
	}

	// fail to produce m2
	pe := &sarama.ProducerError{
		Msg: &sarama.ProducerMessage{
			Metadata: kafkaMessageKey(m2),
		},
		Err: fmt.Errorf("error"),
	}
	s.saramaProducer.errs = append(s.saramaProducer.errs, pe)
	go s.dlq.eventLoop()

	thread1 := make(chan error)
	go func() {
		err := s.dlq.Add(m1)
		thread1 <- err
	}()

	select {
	case <-thread1:
		s.Fail("dlq.Add should block until flush")
	case <-time.After(time.Millisecond):
		break
	}

	thread2 := make(chan error)
	go func() {
		err := s.dlq.Add(m2)
		thread2 <- err
	}()

	// force flush
	time.Sleep(100 * time.Millisecond)
	s.timeLimit <- time.Now()

	select {
	case <-time.After(100 * time.Millisecond):
		s.Fail("Expected thread1 to return nil")
	case err := <-thread1:
		s.NoError(err)
	}

	select {
	case <-time.After(100 * time.Millisecond):
		s.Fail("Expected thread2 to return err")
	case err := <-thread2:
		s.Error(err)
	}

	s.saramaProducer.errs = nil
	s.dlq.Stop()
	s.Equal(0, s.dlq.messageBufferMap.Size())
	s.Equal(0, s.dlq.errorMap.Size())
}
