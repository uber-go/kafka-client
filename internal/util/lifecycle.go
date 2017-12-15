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
	"sync"

	"go.uber.org/zap"
)

// RunLifecycle manages the start/stop lifecycle
// for a runnable implementation
type RunLifecycle struct {
	sync.Mutex
	name    string
	started bool
	stopped bool
	logger  *zap.Logger
}

// NewRunLifecycle returns a lifecycle object that can be
// used by a Runnable implementation to make sure the start/stop
// operations are idempotent
func NewRunLifecycle(name string, logger *zap.Logger) *RunLifecycle {
	return &RunLifecycle{name: name}
}

// Start executes the given action if and on if lifecycle
// state is previously not started or stopped
func (r *RunLifecycle) Start(action func() error) error {
	r.Lock()
	defer r.Unlock()
	if r.stopped {
		r.logger.Fatal("attempt to restart a previously stopped runnable", zap.String("name", r.name))
	}
	if !r.started {
		r.started = true
		if err := action(); err != nil {
			r.stopped = true
			return err
		}
	}
	return nil
}

// Stop stops the given action if and only if lifecycle
// state is previously started
func (r *RunLifecycle) Stop(action func()) {
	r.Lock()
	defer r.Unlock()
	if !r.started {
		r.stopped = true
		return
	}
	if !r.stopped {
		action()
		r.stopped = true
	}
}
