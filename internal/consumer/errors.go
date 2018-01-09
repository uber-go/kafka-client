package consumer

type errorAccumulator []error

func newErrorAccumulator() errorAccumulator {
	return make([]error, 0, 10)
}

// Error returns error strings joined by "\n"
func (acc errorAccumulator) Error() string {
	errStr := ""
	for _, err := range acc {
		errStr += err.Error()
		errStr += "\n"
	}
	return errStr
}

// ToError returns nil if there are no errors stored in the accumulator
func (acc errorAccumulator) ToError() error {
	if len(acc) == 0 {
		return nil
	}
	return acc
}
