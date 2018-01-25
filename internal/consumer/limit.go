package consumer

import "time"

const (
	noLimit = -2
	// defaultLimit set to -1 so that if you use default limit, no messages will be processed.
	defaultLimit              = -1
	defaultLimitCheckInterval = time.Second
)

type topicPartitionLimitMap struct {
	checkInterval time.Duration
	limits        map[TopicPartition]int64
}

func newTopicLimitMap(limit map[TopicPartition]int64) topicPartitionLimitMap {
	return topicPartitionLimitMap{
		limits:        limit,
		checkInterval: defaultLimitCheckInterval,
	}
}

// HasLimits returns true if there are limits set.
func (m *topicPartitionLimitMap) HasLimits() bool {
	return m.limits != nil
}

// SetLimits sets limits for this topic partition limit map.
func (m *topicPartitionLimitMap) SetLimits(limits map[TopicPartition]int64) {
	m.limits = limits
}

// Get returns noLimit if there are no limits set.
// If there are limits set but no limits for this topic partition, then defaultLimit will be used.
// Else, it will return the limit stored in the limits map.
func (m *topicPartitionLimitMap) Get(tp TopicPartition) int64 {
	if m.limits == nil {
		return noLimit
	}

	limit, ok := m.limits[tp]
	if !ok {
		return defaultLimit
	}

	return limit
}
