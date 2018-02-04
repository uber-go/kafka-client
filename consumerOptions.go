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
	"github.com/uber-go/kafka-client/internal/consumer"
)

type (
	// ConsumerOption is the type for optional arguments to the NewConsumer constructor.
	ConsumerOption interface {
		apply(*consumer.Options)
	}

	consumerBuildOptions []ConsumerOption

	partialConsumption struct {
		enabled bool
		errs    *consumerErrorList
	}
)

// EnablePartialConsumption will set the client to return a partial consumer that
// consumes from as many topics/clusters as it could and it may return an error that lists the
// topics that failed to connect to their cluster.
func EnablePartialConsumption() ConsumerOption {
	return &partialConsumption{
		enabled: true,
	}
}

func (p *partialConsumption) apply(opt *consumer.Options) {
	opt.PartialConsumption = p.enabled
}

func (c consumerBuildOptions) addConsumerErrorList(errs *consumerErrorList) {
	for _, opt := range c {
		pe, ok := opt.(*partialConsumption)
		if ok {
			pe.errs = errs
		}
	}
}
