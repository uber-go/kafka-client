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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/golang/protobuf/proto"
)

func TestSaramaConsumer(t *testing.T) {
	mockC := newMockSaramaConsumer()
	c, err := newSaramaConsumer(mockC)
	assert.NoError(t, err)
	assert.NoError(t, c.Close())
	assert.EqualValues(t, 1, atomic.LoadInt64(&mockC.closed))

	// Second close should return no error and not increment closed counter
	assert.NoError(t, c.Close())
	assert.EqualValues(t, 1, atomic.LoadInt64(&mockC.closed))
}

func TestSaramaProducer(t *testing.T) {
	mockP := newMockSaramaProducer()
	c, err := newSaramaProducer(mockP)
	assert.NoError(t, err)
	assert.NoError(t, c.Close())
	assert.EqualValues(t, 1, atomic.LoadInt32(&mockP.closed))

	// Second close should return no error and not increment closed counter
	assert.NoError(t, c.Close())
	assert.EqualValues(t, 1, atomic.LoadInt32(&mockP.closed))
}

func TestSaramaClient(t *testing.T) {
	mock := newMockSaramaClient()
	c, err := newSaramaClient(mock)
	assert.NoError(t, err)
	assert.NoError(t, c.Close())
	assert.EqualValues(t, 1, atomic.LoadInt32(&mock.closed))

	// Second close should return no error and not increment closed counter
	assert.NoError(t, c.Close())
	assert.EqualValues(t, 1, atomic.LoadInt32(&mock.closed))
}

func TestProtobufDLQMetadataDecoder(t *testing.T) {
	dlqMetadata := newDLQMetadata()
	b, err := proto.Marshal(dlqMetadata)
	assert.NoError(t, err)
	decodedDLQMetadata, err := ProtobufDLQMetadataDecoder(b)
	assert.NoError(t, err)
	assert.EqualValues(t, dlqMetadata, decodedDLQMetadata)
}

func TestNoopDLQMetadataDecoder(t *testing.T) {
	dlqMetadata := newDLQMetadata()
	dlqMetadata.Offset = 100
	b, err := proto.Marshal(dlqMetadata)
	assert.NoError(t, err)
	decodedDLQMetadata, err := NoopDLQMetadataDecoder(b)
	assert.NoError(t, err)
	assert.EqualValues(t, newDLQMetadata(), decodedDLQMetadata)
}
