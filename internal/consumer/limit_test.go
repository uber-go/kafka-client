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
	"github.com/stretchr/testify/suite"
	"testing"
)

type TopicPartitionLimitMapTestSuite struct {
	suite.Suite
	limitMap   topicPartitionLimitMap
	noLimitMap topicPartitionLimitMap
}

func TestTopicPartitionLimitMap(t *testing.T) {
	suite.Run(t, new(TopicPartitionLimitMapTestSuite))
}

func (s *TopicPartitionLimitMapTestSuite) SetupTest() {
	s.limitMap = newTopicLimitMap(map[TopicPartition]int64{
		{Topic: "t", Partition: 0}: 100,
	})
	s.noLimitMap = newTopicLimitMap(nil)
}

func (s *TopicPartitionLimitMapTestSuite) TestHasLimits() {
	s.True(s.limitMap.HasLimits())
	s.False(s.noLimitMap.HasLimits())
}

func (s *TopicPartitionLimitMapTestSuite) TestGet() {
	s.EqualValues(noLimit, s.noLimitMap.Get(TopicPartition{Topic: "t", Partition: 1}))
	s.EqualValues(defaultLimit, s.limitMap.Get(TopicPartition{Topic: "t", Partition: 1}))
	s.EqualValues(100, s.limitMap.Get(TopicPartition{Topic: "t", Partition: 0}))
}
