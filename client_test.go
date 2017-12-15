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
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/kafka-client/internal/consumer"
)

type ClientTestSuite struct {
	suite.Suite
}

func TestClientTestSuite(t *testing.T) {
	suite.Run(t, new(ClientTestSuite))
}

func (s *ClientTestSuite) TestBuildSaramaConfig() {
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
	s.Equal(opts.PartitionRcvBufferSize, config.ChannelBufferSize)
	s.Equal(opts.OffsetPolicy, config.Consumer.Offsets.Initial)
	s.Equal(opts.OffsetCommitInterval, config.Consumer.Offsets.CommitInterval)
	s.Equal(opts.MaxProcessingTime, config.Consumer.MaxProcessingTime)
	s.True(config.Consumer.Return.Errors)
	s.Equal(opts.RebalanceDwellTime, config.Group.Offsets.Synchronization.DwellTime)
	s.True(config.Group.Return.Notifications)
	s.Equal(cluster.ConsumerModePartitions, config.Group.Mode)
}
