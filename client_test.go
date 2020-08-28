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

package kafkaclient

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/stretchr/testify/assert"
	"github.com/gig/kafka-client/lib/consumer"
	"github.com/gig/kafka-client/kafka"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type (
	resolverMock struct {
		errs        map[string]error
		clusterToIP map[string][]string
	}

	saramaConsumerConstructorMock struct {
		errRet map[string]error
	}

	saramaProducerConstructorMock struct {
		errRet map[string]error
	}

	clusterConsumerConstructorMock struct {
		errRet map[string]error
	}
)

func (m *saramaConsumerConstructorMock) f(brokers []string, _ string, _ []string, _ *cluster.Config) (sc consumer.SaramaConsumer, err error) {
	key := ""
	for _, broker := range brokers {
		key += broker
	}

	err = m.errRet[key]
	return
}

func (m *saramaProducerConstructorMock) f(brokers []string) (p sarama.SyncProducer, err error) {
	key := ""
	for _, broker := range brokers {
		key += broker
	}

	err = m.errRet[key]
	return
}

func (m *resolverMock) ResolveIPForCluster(cluster string) (ip []string, err error) {
	err = m.errs[cluster]
	ip = m.clusterToIP[cluster]
	return
}

func (m *resolverMock) ResolveClusterForTopic(topic string) (cluster []string, err error) {
	err = m.errs[topic]
	return
}

func (m *clusterConsumerConstructorMock) f(_ string, cluster string, _ *consumer.Options, _ kafka.ConsumerTopicList, _ chan kafka.Message, _ consumer.SaramaConsumer, _ map[string]consumer.DLQ, _ tally.Scope, _ *zap.Logger) (kc kafka.Consumer, err error) {
	err = m.errRet[cluster]
	return
}

func TestBuildSaramaConfig(t *testing.T) {
	opts := &consumer.Options{
		RcvBufferSize:          128,
		PartitionRcvBufferSize: 64,
		OffsetCommitInterval:   time.Minute,
		RebalanceDwellTime:     time.Hour,
		MaxProcessingTime:      time.Second,
		OffsetPolicy:           sarama.OffsetNewest,
		ConsumerMode:           cluster.ConsumerModePartitions,
	}
	config := buildSaramaConfig(opts)
	assert.Equal(t, opts.PartitionRcvBufferSize, config.ChannelBufferSize)
	assert.Equal(t, opts.OffsetPolicy, config.Consumer.Offsets.Initial)
	assert.Equal(t, opts.OffsetCommitInterval, config.Consumer.Offsets.CommitInterval)
	assert.Equal(t, opts.MaxProcessingTime, config.Consumer.MaxProcessingTime)
	assert.True(t, config.Consumer.Return.Errors)
	assert.Equal(t, opts.RebalanceDwellTime, config.Group.Offsets.Synchronization.DwellTime)
	assert.True(t, config.Group.Return.Notifications)
	assert.Equal(t, cluster.ConsumerModePartitions, config.Group.Mode)
}
