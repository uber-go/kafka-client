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

package util

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
)

type RunLifecycleTestSuite struct {
	suite.Suite
}

func TestRunLifecycleTestSuite(t *testing.T) {
	suite.Run(t, new(RunLifecycleTestSuite))
}

func (s *RunLifecycleTestSuite) TestSuccessFlow() {
	state := ""
	lc := NewRunLifecycle("test")
	err := lc.Start(func() error {
		state = "started"
		return nil
	})
	s.NoError(err)
	s.Equal("started", state)
	lc.Stop(func() { state = "stopped" })
	s.Equal("stopped", state)
}

func (s *RunLifecycleTestSuite) TestStartError() {
	state := ""
	lc := NewRunLifecycle("test")
	err := lc.Start(func() error {
		state = "started"
		return fmt.Errorf("test error")
	})
	s.Error(err)
	s.Equal("started", state)
}

func (s *RunLifecycleTestSuite) TestMultipleStarts() {
	state := "test"
	lc := NewRunLifecycle("test")
	for i := 0; i < 10; i++ {
		err := lc.Start(func() error {
			state = state + "_started"
			return nil
		})
		s.NoError(err)
		s.Equal("test_started", state)
	}
}

func (s *RunLifecycleTestSuite) TestMultipleStops() {
	state := ""
	lc := NewRunLifecycle("test")
	lc.Start(func() error {
		state = "started"
		return nil
	})
	for i := 0; i < 10; i++ {
		lc.Stop(func() { state = state + "_stopped" })
		s.Equal("started_stopped", state)
	}
}

func (s *RunLifecycleTestSuite) TestStopBeforeStart() {
	state := "not_started"
	lc := NewRunLifecycle("test")
	lc.Stop(func() { state = "stopped" })
	s.Equal("not_started", state)
	s.True(lc.stopped)
}
