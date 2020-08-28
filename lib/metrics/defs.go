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

package metrics

// Counters
const (
	KafkaConsumerStarted = "kafka.consumer.started"
	KafkaConsumerStopped = "kafka.consumer.stopped"

	KafkaPartitionStarted    = "kafka.partition.started"
	KafkaPartitionStopped    = "kafka.partition.stopped"
	KafkaPartitionMessagesIn = "kafka.partition.messages-in"

	KafkaDLQStarted       = "kafka.dlq.started"
	KafkaDLQStopped       = "kafka.dlq.stopped"
	KafkaDLQMessagesOut   = "kafka.dlq.messages-out"
	KafkaDLQErrors        = "kafka.dlq.errors"
	KafkaDLQMetadataError = "kafka.dlq.metadata-error"

	KafkaPartitionAckMgrDups     = "kafka.partition.ackmgr.duplicates"
	KafkaPartitionAckMgrSkipped  = "kafka.partition.ackmgr.skipped"
	KafkaPartitionAckMgrListFull = "kafka.partition.ackmgr.list-full-error"
	KafkaPartitionGetAckIDErrors = "kafka.partition.ackmgr.get-ackid-error"
	KafkaPartitionAck            = "kafka.partition.ack"
	KafkaPartitionAckErrors      = "kafka.partition.ackmgr.ack-error"
	KafkaPartitionNack           = "kafka.partition.nack"
	KafkaPartitionNackErrors     = "kafka.partition.ackmgr.nack-error"
	KafkaPartitionRebalance      = "kafka.partition.rebalance"
)

// Gauges
const (
	KafkaPartitionTimeLag            = "kafka.partition.time-lag"
	KafkaPartitionOffsetLag          = "kafka.partition.offset-lag"
	KafkaPartitionOffsetFreshnessLag = "kafka.partition.freshness-lag"
	KafkaPartitionReadOffset         = "kafka.partition.read-offset"
	KafkaPartitionCommitOffset       = "kafka.partition.commit-offset"
	KafkaPartitionOwnedCount         = "kafka.partition.owned.count"
	KafkaPartitionOwned              = "kafka.partition.owned"
)
