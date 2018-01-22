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
	"testing"

	"github.com/stretchr/testify/suite"
)

type ResolverTestSuite struct {
	suite.Suite
	clusterBrokerMap map[string][]string
	topicClusterMap  map[string][]string
}

func TestClientTestSuite(t *testing.T) {
	suite.Run(t, new(ResolverTestSuite))
}

func (s *ResolverTestSuite) SetupTest() {
	s.clusterBrokerMap = map[string][]string{
		"cluster_az1": {"127.0.0.1", "127.0.0.2", "127.0.0.3"},
		"cluster_az2": {"127.1.1.1"},
		"cluster_az3": {"127.2.2.2", "127.2.2.3"},
	}
	s.topicClusterMap = map[string][]string{
		"topic1": {"cluster_az1"},
	}
}

func (s *ResolverTestSuite) TestResolveIPForCluster() {
	clusterBrokers := s.clusterBrokerMap
	topicClusterMap := s.topicClusterMap
	resolver := NewStaticNameResolver(topicClusterMap, clusterBrokers)
	s.NotNil(resolver)
	_, err := resolver.ResolveIPForCluster("foobar")
	s.Equal(errNoBrokersForCluster, err)
	for k, v := range clusterBrokers {
		brokers, err := resolver.ResolveIPForCluster(k)
		s.NoError(err)
		s.Equal(v, brokers)
	}
}

func (s *ResolverTestSuite) TestResolveClusterForTopic() {
	clusterBrokers := s.clusterBrokerMap
	topicClusterMap := s.topicClusterMap
	resolver := NewStaticNameResolver(topicClusterMap, clusterBrokers)
	s.NotNil(resolver)
	_, err := resolver.ResolveClusterForTopic("foobar")
	s.Equal(errNoClustersForTopic, err)
	for k, v := range topicClusterMap {
		cluster, err := resolver.ResolveClusterForTopic(k)
		s.NoError(err)
		s.Equal(v, cluster)
	}
}
