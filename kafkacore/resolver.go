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

package kafkacore

import (
	"errors"
	"sync"
)

type (
	// NameResolver is an interface that will be used by the consumer library to resolve
	// (1) topic to cluster name and (2) cluster name to broker IP addresses.
	// Implementations of KafkaNameResolver should be threadsafe.
	NameResolver interface {
		// ResolveCluster returns a list of IP addresses for the brokers
		ResolveIPForCluster(cluster string) ([]string, error)
		// ResolveClusterForTopic returns the logical cluster names corresponding to a topic name
		//
		// It is possible for a topic to exist on multiple clusters in order to
		// transparently handle topic migration between clusters.
		// TODO (gteo): Remove to simplify API because not needed anymore
		ResolveClusterForTopic(topic string) ([]string, error)
	}

	// staticResolver is an implementation of NameResolver
	// that's backed by a static map of clusters to list of brokers
	// and a map of topics to cluster
	staticResolver struct {
		sync.RWMutex
		topicsToCluster  map[string][]string
		clusterToBrokers map[string][]string
	}
)

// errNoBrokersForCluster is returned when no brokers can be found for a cluster
var errNoBrokersForCluster = errors.New("no brokers found for cluster")

// errNoClustersForTopic is returned when no cluster can be found for a topic
var errNoClustersForTopic = errors.New("no cluster found for topic")

// NewStaticNameResolver returns a instance of NameResolver that relies
// on a static map of topic to list of brokers and map of topics to cluster
func NewStaticNameResolver(
	topicsToCluster map[string][]string,
	clusterToBrokers map[string][]string,
) NameResolver {
	return &staticResolver{
		topicsToCluster:  topicsToCluster,
		clusterToBrokers: clusterToBrokers,
	}
}

// ResolveIPForCluster returns list of IP addresses by cluster name by looking up in
// the clusterToBrokers map passed into the NewStaticNameResolver constructor.
func (r *staticResolver) ResolveIPForCluster(cluster string) ([]string, error) {
	r.RLock()
	defer r.RUnlock()

	if brokers, ok := r.clusterToBrokers[cluster]; ok {
		return brokers, nil
	}
	return nil, errNoBrokersForCluster
}

// ResolveClusterForTopic resolves the cluster name for a specific topic by looking
// up in the topicsToCluster map passed into the NewStaticNameResolver constructor.
func (r *staticResolver) ResolveClusterForTopic(topic string) ([]string, error) {
	r.RLock()
	defer r.RUnlock()

	if cluster, ok := r.topicsToCluster[topic]; ok {
		return cluster, nil
	}
	return nil, errNoClustersForTopic
}
