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
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/uber-go/kafka-client/internal/consumer"
	"github.com/uber-go/kafka-client/kafka"
)

func TestDLQConsumerOptions(t *testing.T) {
	testTopics := make([]kafka.ConsumerTopic, 0, 10)
	testTopics = append(testTopics, *new(kafka.ConsumerTopic))
	testTopics = append(testTopics, *new(kafka.ConsumerTopic))
	consumerOption := WithDLQTopics(testTopics)
	options := consumer.DefaultOptions()
	consumerOption.apply(options)
	assert.Equal(t, 2, len(options.OtherConsumerTopics))
}

func TestRetryConsumerOptions(t *testing.T) {
	testTopics := make([]kafka.ConsumerTopic, 0, 10)
	testTopics = append(testTopics, *new(kafka.ConsumerTopic))
	testTopics = append(testTopics, *new(kafka.ConsumerTopic))
	consumerOption := WithRetryTopics(testTopics)
	options := consumer.DefaultOptions()
	consumerOption.apply(options)
	assert.Equal(t, 2, len(options.OtherConsumerTopics))
}

func TestTLSOption(t *testing.T) {
	tlsConfig := new(tls.Config)
	consumerOption := WithTLS(tlsConfig)
	options := consumer.DefaultOptions()
	consumerOption.apply(options)
	assert.True(t, options.TLS.Enable)
	assert.NotNil(t, options.TLS.Config)
}
