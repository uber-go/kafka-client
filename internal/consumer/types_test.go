package consumer

import (
	"github.com/stretchr/testify/assert"
	"sync/atomic"
	"testing"
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
	mockP := newMockDLQProducer()
	c, err := newSaramaProducer(mockP)
	assert.NoError(t, err)
	assert.NoError(t, c.Close())
	assert.EqualValues(t, 1, atomic.LoadInt64(&mockP.closed))

	// Second close should return no error and not increment closed counter
	assert.NoError(t, c.Close())
	assert.EqualValues(t, 1, atomic.LoadInt64(&mockP.closed))
}
