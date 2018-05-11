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
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/uber-go/kafka-client/internal/list"
	"github.com/uber-go/kafka-client/internal/metrics"
	"github.com/uber-go/kafka-client/internal/util"
	"github.com/uber-go/kafka-client/kafka"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type (
	topicPartition struct {
		partition int32
		Topic
	}

	// PartitionConsumerFactory is a factory method for returning PartitionConsumer.
	// NewPartitionConsumer returns an unbounded partition consumer.
	// NewRangePartitionConsumer returns a range partition consumer.
	PartitionConsumerFactory func(
		topic Topic,
		sarama SaramaConsumer,
		pConsumer cluster.PartitionConsumer,
		options *Options,
		msgCh chan kafka.Message,
		dlq DLQ,
		scope tally.Scope,
		logger *zap.Logger) PartitionConsumer

	// PartitionConsumer is the consumer for a specific
	// kafka partition
	PartitionConsumer interface {
		Start() error
		Stop()
		Drain(time.Duration)
		ResetOffset(kafka.OffsetRange) error
	}

	partitionConsumer struct {
		topicPartition topicPartition
		msgCh          chan kafka.Message
		ackMgr         *ackManager
		sarama         SaramaConsumer
		pConsumer      cluster.PartitionConsumer
		dlq            DLQ
		options        *Options
		tally          tally.Scope
		logger         *zap.Logger
		stopC          chan struct{}
		lifecycle      *util.RunLifecycle
	}

	// rangePartitionConsumer consumes only a specific offset range.
	// Only a single offset range can be consumed at a single time.
	// If an offset range is currently being consumed, calls to ResetOffset
	// will block until all messages in the previous offset range has been received
	// and acked/nacked.
	rangePartitionConsumer struct {
		*partitionConsumer
		offsetRangeC chan kafka.OffsetRange
	}
)

func (t topicPartition) MarshalLogObject(e zapcore.ObjectEncoder) error {
	t.Topic.MarshalLogObject(e)
	e.AddInt32("partition", t.partition)
	return nil
}

// NewPartitionConsumer returns a kafka consumer that can
// read messages from a given [ topic, partition ] tuple
func NewPartitionConsumer(
	topic Topic,
	sarama SaramaConsumer,
	pConsumer cluster.PartitionConsumer,
	options *Options,
	msgCh chan kafka.Message,
	dlq DLQ,
	scope tally.Scope,
	logger *zap.Logger) PartitionConsumer {
	return newPartitionConsumer(
		topic,
		sarama,
		pConsumer,
		options,
		msgCh,
		dlq,
		scope,
		logger,
	)
}

func newPartitionConsumer(
	topic Topic,
	sarama SaramaConsumer,
	pConsumer cluster.PartitionConsumer,
	options *Options,
	msgCh chan kafka.Message,
	dlq DLQ,
	scope tally.Scope,
	logger *zap.Logger) *partitionConsumer {
	maxUnAcked := options.Concurrency + 2*options.RcvBufferSize + 1
	name := fmt.Sprintf("%v-partition-%v", pConsumer.Topic(), pConsumer.Partition())
	scope = scope.Tagged(map[string]string{"partition": strconv.Itoa(int(pConsumer.Partition()))})
	fmt.Println("##### creating partition")
	return &partitionConsumer{
		topicPartition: topicPartition{
			partition: pConsumer.Partition(),
			Topic:     topic,
		},
		sarama:    sarama,
		pConsumer: pConsumer,
		options:   options,
		msgCh:     msgCh,
		dlq:       dlq,
		tally:     scope,
		logger:    logger.With(zap.String("cluster", topic.Cluster), zap.String("topic", topic.Name), zap.Int32("partition", pConsumer.Partition())),
		stopC:     make(chan struct{}),
		ackMgr:    newAckManager(maxUnAcked, scope, logger),
		lifecycle: util.NewRunLifecycle(name),
	}
}

// NewRangePartitionConsumer returns a kafka consumer that can
// read messages from a given [ topic, partition ] tuple
func NewRangePartitionConsumer(
	topic Topic,
	sarama SaramaConsumer,
	pConsumer cluster.PartitionConsumer,
	options *Options,
	msgCh chan kafka.Message,
	dlq DLQ,
	scope tally.Scope,
	logger *zap.Logger) PartitionConsumer {
	return newRangePartitionConsumer(newPartitionConsumer(
		topic,
		sarama,
		pConsumer,
		options,
		msgCh,
		dlq,
		scope,
		logger,
	))
}

func newRangePartitionConsumer(consumer *partitionConsumer) *rangePartitionConsumer {
	return &rangePartitionConsumer{
		partitionConsumer: consumer,
		offsetRangeC:      make(chan kafka.OffsetRange),
	}
}

// Start starts the consumer
func (p *partitionConsumer) Start() error {
	fmt.Println("##### staring partition")
	return p.lifecycle.Start(func() error {
		go p.messageLoop(nil)
		go p.commitLoop()
		p.tally.Counter(metrics.KafkaPartitionStarted).Inc(1)
		p.logger.Info("partition consumer started", zap.Object("topicPartition", p.topicPartition))
		return nil
	})
}

// Stop stops the consumer immediately
// Does not wait for inflight messages
// to be drained
func (p *partitionConsumer) Stop() {
	p.stop(time.Duration(0))
}

// Drain gracefully stops this consumer
// - Waits for inflight messages to be drained
// - Checkpoints the latest offset to the broker
// - Stops the underlying consumer
func (p *partitionConsumer) Drain(d time.Duration) {
	p.stop(d)
}

func (p *partitionConsumer) ResetOffset(offsetRange kafka.OffsetRange) error {
	if offsetRange.HighOffset != -1 {
		p.logger.Warn("partition consumer unable to set highwatermark")
	}
	p.sarama.ResetPartitionOffset(p.topicPartition.Topic.Name, p.topicPartition.partition, offsetRange.LowOffset, "")
	return nil
}

// messageLoop is the message read loop for this consumer
// TODO (venkat): maintain a pre-allocated pool of Messages
func (p *partitionConsumer) messageLoop(offsetRange *kafka.OffsetRange) {
	var drain bool
	if offsetRange != nil {
		drain = true
	}
	p.logger.Info("partition consumer message loop started")
	for {
		select {
		case m, ok := <-p.pConsumer.Messages():
			if !ok {
				p.logger.Info("partition consumer message channel closed")
				p.Drain(p.options.MaxProcessingTime)
				return
			}

			if drain && m.Offset != offsetRange.LowOffset {
				p.logger.Debug("partition consumer drain message", zap.Object("offsetRange", offsetRange), zap.Int64("offset", m.Offset))
				continue
			}
			if drain {
				p.logger.Debug("partition consumer drain message complete", zap.Object("offsetRange", offsetRange), zap.Int64("offset", m.Offset))
			}
			drain = false
			p.delayMsg(m)
			lag := time.Now().Sub(m.Timestamp)
			p.tally.Gauge(metrics.KafkaPartitionTimeLag).Update(float64(lag))
			p.tally.Gauge(metrics.KafkaPartitionReadOffset).Update(float64(m.Offset))
			p.tally.Counter(metrics.KafkaPartitionMessagesIn).Inc(1)
			p.tally.Gauge(metrics.KafkaPartitionOffsetFreshnessLag).Update(float64(p.pConsumer.HighWaterMarkOffset() - 1 - m.Offset))
			p.deliver(m)

			if offsetRange != nil && m.Offset >= offsetRange.HighOffset {
				p.logger.Info("partition consumer message loop reached range", zap.Int64("offset", m.Offset), zap.Object("offsetRange", offsetRange))
				return
			}
		case <-p.stopC:
			p.logger.Info("partition consumer message loop stopped")
			return
		}
	}
}

// delayMsg when Topic.Delay is not zero, it try to postpone the consumption of the msg for topic.Delay duration
// if the current timestamp is too soon compare to the m.Timestamp.
func (p *partitionConsumer) delayMsg(m *sarama.ConsumerMessage) {
	delay := p.topicPartition.Delay
	// check non-zero msg delay
	if delay > 0 {
		sleepDuration := delay + m.Timestamp.Sub(time.Now())
		if sleepDuration > 0 {
			p.logger.Debug("delay msg delivery", zap.Duration("sleepDuration", sleepDuration), zap.Int64("offset", m.Offset))
			time.Sleep(sleepDuration)
		}
	}
}

// commitLoop periodically checkpoints the offsets with broker
func (p *partitionConsumer) commitLoop() {
	ticker := time.NewTicker(p.options.MaxProcessingTime)
	p.logger.Info("partition consumer commit loop started")
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.markOffset()
		case <-p.stopC:
			p.markOffset()
			p.logger.Info("partition consumer commit loop stopped")
			return
		}
	}
}

// markOffset checkpoints the latest offset
func (p *partitionConsumer) markOffset() {
	latestOff := p.ackMgr.CommitLevel()
	if latestOff >= 0 {
		p.sarama.MarkPartitionOffset(p.topicPartition.Topic.Name, p.topicPartition.partition, latestOff, "")
		p.tally.Gauge(metrics.KafkaPartitionCommitOffset).Update(float64(latestOff))
		// Highwatermark is the next offset that will be produced.
		// Kafka contains offsets 100 - 200, HighWaterMark=201, (consumed) latestOff=150, 200-150 = 50 = 201-1-150 unconsumed messages
		backlog := math.Max(float64(0), float64(p.pConsumer.HighWaterMarkOffset()-1-latestOff))
		p.tally.Gauge(metrics.KafkaPartitionOffsetLag).Update(backlog)
		//p.logger.Debug("partition consumer mark kafka checkpoint", zap.Int64("offset", latestOff))
	}
}

// deliver delivers a message through the consumer channel
func (p *partitionConsumer) deliver(scm *sarama.ConsumerMessage) {
	metadata, err := p.topicPartition.DLQMetadataDecoder(scm.Key)
	if err != nil {
		p.logger.Fatal("partition consumer unable to marshal dlq metadata from message key", zap.Error(err))
		return
	}

	ackID, err := p.trackOffset(scm.Offset)
	if err != nil {
		p.logger.Error("failed to track offset for message", zap.Error(err))
		return
	}

	msg := newMessage(scm, ackID, p.ackMgr, p.dlq, *metadata)
	select {
	case p.msgCh <- msg:
		return
	case <-p.stopC:
		return
	}
}

// trackOffset adds the given offset to the list of unacked
func (p *partitionConsumer) trackOffset(offset int64) (ackID, error) {
	for {
		id, err := p.ackMgr.GetAckID(offset)
		if err != nil {
			if err != list.ErrCapacity {
				return ackID{}, err
			}
			p.tally.Counter(metrics.KafkaPartitionAckMgrListFull).Inc(1)
			p.logger.Warn("partition consumer ackMgr list ran out of capacity")
			// list can never be out of capacity if maxOutstanding
			// is configured correctly for ackMgr, but if not, we have no
			// option but to spin in this case
			if p.sleep(time.Millisecond * 100) {
				return ackID{}, fmt.Errorf("consumer stopped")
			}
			continue
		}
		return id, nil
	}
}

func (p *partitionConsumer) stop(d time.Duration) {
	p.lifecycle.Stop(func() {
		close(p.stopC)
		time.Sleep(d)
		p.markOffset()
		p.pConsumer.Close()
		p.tally.Counter(metrics.KafkaPartitionStopped).Inc(1)
		p.logger.Info("partition consumer stopped", zap.Object("topicPartition", p.topicPartition))
	})
}

func (p *partitionConsumer) sleep(d time.Duration) bool {
	select {
	case <-time.After(d):
		return false
	case <-p.stopC:
		return true
	}
}

func (p *rangePartitionConsumer) Start() error {
	return p.lifecycle.Start(func() error {
		go p.offsetLoop()
		go p.commitLoop()
		p.tally.Counter(metrics.KafkaPartitionStarted).Inc(1)
		p.logger.Info("range partition consumer started", zap.Object("topicPartition", p.topicPartition))
		return nil
	})
}

func (p *rangePartitionConsumer) offsetLoop() {
	p.logger.Info("range partition consumer offset loop started", zap.Object("topicPartition", p.topicPartition))
	for {
		select {
		case ntf, ok := <-p.offsetRangeC:
			p.logger.Debug("range partition consumer received offset range", zap.Object("topicPartition", p.topicPartition), zap.Object("offsetRange", ntf))
			if !ok {
				p.logger.Info("range partition consumer offset range channel closed")
				p.Drain(p.options.MaxProcessingTime)
				return
			}
			p.logger.Debug("range partition consumer ack mgr reset started", zap.Object("offsetRange", ntf))
			p.ackMgr.Reset() // Reset blocks until all messages that are currently inflight have been acked/nacked.
			p.sarama.ResetPartitionOffset(p.topicPartition.Name, p.topicPartition.partition, ntf.LowOffset-1, "")
			p.logger.Debug("range partition consumer ack mgr reset completed", zap.Object("offsetRange", ntf))
			p.messageLoop(&ntf)
			p.logger.Debug("range partition consumer message loop completed", zap.Object("offsetRange", ntf))
		case <-p.stopC:
			p.logger.Info("range partition consumer offset loop stopped", zap.Object("topicPartition", p.topicPartition))
			return
		}
	}
}

// ResetOffset will reset the consumer offset for the specified partition.
func (p *rangePartitionConsumer) ResetOffset(offsetRange kafka.OffsetRange) error {
	if offsetRange.HighOffset == -1 {
		return errors.New("rangePartitionConsumer expects a highwatermark when resetting offset")
	}

	// If it has been closed, return error.
	select {
	case _, ok := <-p.stopC:
		if !ok {
			return errors.New("unable to ResetOffset for rangePartitionConsumer has been closed")
		}
	case <-time.After(time.Nanosecond):
		break
	}

	select {
	case p.offsetRangeC <- offsetRange:
		return nil
	default:
		return errors.New("failed to merge due to ongoing merge")
	}
}
