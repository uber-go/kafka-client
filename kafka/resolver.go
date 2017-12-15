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

package kafka

import "errors"

// staticResolver is an implementation of ClusterNameResolver
// that's backed by a static map of clusters to list of brokers
type staticResolver struct {
	clusterToBrokers map[string][]string
}

// ErrNoBrokers is returned when no brokers can be found for a cluster
var ErrNoBrokers = errors.New("no brokers found for cluster")

// NewStaticNameResolver returns a instance of TopicNameResolver that relies
// on a static map of topic to list of brokers
func NewStaticNameResolver(clusterToBrokers map[string][]string) ClusterNameResolver {
	return &staticResolver{
		clusterToBrokers: clusterToBrokers,
	}
}

func (r *staticResolver) Resolve(topic string) ([]string, error) {
	if brokers, ok := r.clusterToBrokers[topic]; ok {
		return brokers, nil
	}
	return nil, ErrNoBrokers
}
