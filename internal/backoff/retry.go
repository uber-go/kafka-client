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

package backoff

import (
	"time"
)

type (
	// Operation to retry
	Operation func() error

	// IsRetryable handler can be used to exclude certain errors during retry
	IsRetryable func(error) bool
)

// Retry function can be used to wrap any call with retry logic using the passed in policy
func Retry(operation Operation, policy RetryPolicy, isRetryable IsRetryable) error {
	var err error
	var next time.Duration

	r := NewRetrier(policy, SystemClock)
	for {
		// operation completed successfully.  No need to retry.
		if err = operation(); err == nil {
			return nil
		}

		if next = r.NextBackOff(); next == done {
			return err
		}
		// Check if the error is retryable
		if isRetryable != nil && !isRetryable(err) {
			return err
		}

		time.Sleep(next)
	}
}

// IgnoreErrors can be used as IsRetryable handler for Retry function to exclude certain errors from the retry list
func IgnoreErrors(errorsToExclude []error) func(error) bool {
	return func(err error) bool {
		for _, errorToExclude := range errorsToExclude {
			if err == errorToExclude {
				return false
			}
		}

		return true
	}
}
