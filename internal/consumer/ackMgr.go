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
	"time"

	"github.com/uber-go/kafka-client/internal/list"
	"github.com/uber-go/kafka-client/internal/metrics"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const (
	resetCheckInterval = time.Second
)

type (
	ackID struct {
		listAddr list.Address
		msgSeq   int64
	}
	ackManager struct {
		unackedSeqList *threadSafeSortedList
		logger         *zap.Logger
		tally          tally.Scope
	}
	threadSafeSortedList struct {
		sync.Mutex
		list      *list.IntegerList
		lastValue int64
		tally     tally.Scope
	}
)

// newAckManager returns a new instance of AckManager. The returned
// ackManager can be used by a kafka partitioned consumer to keep
// track of the current commit level for the partition. The ackManager
// is only needed when consuming and processing kafka messages in
// parallel (i.e. multiple goroutines), which can cause out of order
// message completion.
//
// In the description below, the terms seqNum and offset are used interchangeably
// Within the ackManager, offsets are nothing but message seqNums
//
// Usage:
// - For every new message, call ackMgr.GetAckID(msgSeqNum) before processing the message
// - After processing, call ackMgr.Ack(ackID) to mark the message as processed
// - If the message processing fails, call ackMgr.Nack(ackID) to skip the message
//   and move it into an error queue (not supported yet)
// - Periodically, call ackMgr.CommitLevel() to retrieve / checkpoint the safe commit level
//
// Assumptions:
//  - The first msgSeq added is considered as the beginSeqNum for this ackManager
//  - Any message with seqNum less than the most recent seqNum is considered a duplicate and ignored
//  - There CANNOT be more than maxOutstanding messages that are unacked at any given point of time
//  - Call ackMgr.Ack is an acknowledgement to move the seqNum past the acked message
//
// Implementation Notes:
// The implementation works by keeping track of unacked seqNums in a doubly linked list. When a
// message is acked, its removed from the linked list. The head of the linked list is the unacked
// message with the lowest seqNum. So, any offset less that that is a safe commit checkpoint.
//
// Params:
//   maxOutstanding - max number of unacked messages at any given point in time
//   scope / logger - metrics / logging client
func newAckManager(maxOutstanding int, scope tally.Scope, logger *zap.Logger) *ackManager {
	return &ackManager{
		tally:          scope,
		logger:         logger,
		unackedSeqList: newThreadSafeSortedList(maxOutstanding, scope),
	}
}

// GetAckID adds the given seqNum to the list of unacked seqNums
// and returns a opaque AckID that can be used to identify this
// message when its subsequently acked or nacked
// Returns an error if the msgSeqNum is unexpected
func (mgr *ackManager) GetAckID(msgSeq int64) (ackID, error) {
	addr, err := mgr.unackedSeqList.Add(msgSeq)
	if err != nil {
		mgr.tally.Counter(metrics.KafkaPartitionGetAckIDErrors)
		if err != list.ErrCapacity {
			// list.ErrCapacity is handled gracefully so no need to log error.
			mgr.logger.Error("GetAckID() error", zap.Int64("rcvdSeq", msgSeq), zap.Error(err))
		}
		return ackID{}, err
	}
	return newAckID(addr, msgSeq), nil
}

// Ack marks the given msgSeqNum as processed
func (mgr *ackManager) Ack(id ackID) {
	err := mgr.unackedSeqList.Remove(id.listAddr, id.msgSeq)
	if err != nil {
		mgr.tally.Counter(metrics.KafkaPartitionAckErrors)
		mgr.logger.Error("ack error: list remove failed", zap.Error(err))
	}
}

// Nack marks the given msgSeqNum as processed, the expectation
// is for the caller to move the message to an error queue
// before calling this
func (mgr *ackManager) Nack(id ackID) {
	mgr.Ack(id)
	mgr.tally.Counter(metrics.KafkaPartitionNacks)
}

// CommitLevel returns the seqNum that can be
// used as a safe commit checkpoint. Returns value
// less than zero if there is no safe checkpoint yet
func (mgr *ackManager) CommitLevel() int64 {
	unacked, err := mgr.unackedSeqList.PeekHead()
	if err != nil {
		if err != list.ErrEmpty {
			mgr.logger.Fatal("commitLevel error: list peekHead failed", zap.Error(err))
		}
		return mgr.unackedSeqList.LastValue()
	}
	return unacked - 1
}

// Reset blocks until the list is empty and the offsets have been reset.
func (mgr *ackManager) Reset() {
	mgr.unackedSeqList.Reset()
}

// newAckID returns a an ackID with the given params
func newAckID(addr list.Address, value int64) ackID {
	return ackID{listAddr: addr, msgSeq: value}
}

// newThreadSafeSortedList returns a new instance of thread safe
// integer list that expects its input to be received in a sorted
// order
func newThreadSafeSortedList(maxOutstanding int, scope tally.Scope) *threadSafeSortedList {
	list := list.NewIntegerList(maxOutstanding)
	return &threadSafeSortedList{list: list, lastValue: -1, tally: scope}
}

// PeekHead returns the value at the head of the list, if it exist
func (l *threadSafeSortedList) PeekHead() (int64, error) {
	l.Lock()
	defer l.Unlock()
	return l.list.PeekHead()
}

func (l *threadSafeSortedList) LastValue() int64 {
	l.Lock()
	value := l.lastValue
	l.Unlock()
	return value
}

// Remove removes the entry at the given address, if and if only if
// the entry has value equal to the given value
func (l *threadSafeSortedList) Remove(addr list.Address, value int64) error {
	l.Lock()
	defer l.Unlock()
	got, err := l.list.Get(addr)
	if err != nil {
		return err
	}
	if value != got {
		return fmt.Errorf("address / value mismatch, expected value of %v but got %v", value, got)
	}
	return l.list.Remove(addr)
}

// Add adds the value to the end of the list. The value MUST be
// greater than the last value added to this list to maintain
// the sorted order. If not, an error is returned
func (l *threadSafeSortedList) Add(value int64) (list.Address, error) {
	l.Lock()
	defer l.Unlock()
	if value <= l.lastValue {
		l.tally.Counter(metrics.KafkaPartitionAckMgrDups).Inc(1)
		return list.Null, fmt.Errorf("new value %v is not greater than last stored value %v", value, l.lastValue)
	}
	skipped := value - l.lastValue - 1
	if skipped > 0 {
		l.tally.Counter(metrics.KafkaPartitionAckMgrSkipped).Inc(skipped)
	}
	addr, err := l.list.Add(value)
	if err == nil {
		l.lastValue = value
	}
	return addr, err
}

// Reset blocks until the list is empty then sets lastValue to -1.
func (l *threadSafeSortedList) Reset() {
	doneC := make(chan struct{})
	checkInterval := time.NewTicker(resetCheckInterval)
	go func() {
		for {
			select {
			case <-checkInterval.C:
				l.Lock()
				if l.list.Empty() {
					l.lastValue = -1
					close(doneC)
				}
				l.Unlock()
			case <-doneC:
				return
			}
		}
	}()
	<-doneC // block until the list is reset
	checkInterval.Stop()
}
