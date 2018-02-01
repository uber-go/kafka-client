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
	"os"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/uber-go/kafka-client/internal/consumer"
	"github.com/uber-go/kafka-client/kafka"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

type (
	// Client refers to the kafka client. Serves as
	// the entry point to producing or consuming
	// messages from kafka
	Client struct {
		tally    tally.Scope
		logger   *zap.Logger
		resolver kafka.NameResolver

		saramaSyncProducerConstructor func([]string) (sarama.SyncProducer, error)
		saramaConsumerConstructor     func([]string, string, []string, *cluster.Config) (consumer.SaramaConsumer, error)
		clusterConsumerConstructor    func(string, string, *consumer.Options, kafka.ConsumerTopicList, chan kafka.Message, consumer.SaramaConsumer, map[string]consumer.DLQ, tally.Scope, *zap.Logger) (kafka.Consumer, error)
	}
)

// New returns a new kafka client
func New(resolver kafka.NameResolver, logger *zap.Logger, scope tally.Scope) *Client {
	return &Client{
		resolver: resolver,
		logger:   logger,
		tally:    scope,
		saramaSyncProducerConstructor: consumer.NewSaramaProducer,
		saramaConsumerConstructor:     consumer.NewSaramaConsumer,
		clusterConsumerConstructor:    consumer.NewClusterConsumer,
	}
}

// NewConsumer returns a new instance of kafka consumer.
//
// It is possible for NewConsumer to start a consumer which consumes from a subset of topics.
// If partial construction has occurred, an error will be returned and you can get a list of topics that are not
// being consumed by using the HasConsumerBuildError() function.
func (c *Client) NewConsumer(config *kafka.ConsumerConfig, consumerOpts ...ConsumerOption) (kafka.Consumer, error) {
	opts := buildOptions(config, consumerOpts...)
	b := newConsumerBuilder(c.logger, c.tally, config, opts)
	b.resolveBrokers(c.resolver)
	b.buildSaramaProducerMap(c.saramaSyncProducerConstructor)
	b.buildSaramaConsumerMap(c.saramaConsumerConstructor)
	if err := b.buildClusterConsumerMap(c.clusterConsumerConstructor); err != nil {
		return nil, err
	}

	return b.build()
}

func clientID() string {
	name, err := os.Hostname()
	if err != nil {
		name = "unknown-kafka-client"
	}
	return name
}
