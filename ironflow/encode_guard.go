package ironflow

import (
	"encoding/json"
	"fmt"
)

// codeSerializationError marks a run whose result or step output JSON cannot
// encode (NaN, ±Inf, a channel, a function, a cycle). Retrying runs the same
// code and fails the same way, so the failure is not retryable.
const codeSerializationError = "SERIALIZATION_ERROR"

// encodableSteps returns the longest prefix of steps that JSON can encode and,
// when it stops early, the failure for the first step it cannot. The prefix
// keeps completed side effects from running again (#2873).
func encodableSteps(steps []*StepResult) ([]*StepResult, *PushError) {
	for i, s := range steps {
		if _, err := json.Marshal(s); err != nil {
			return steps[:i], &PushError{
				Message:   fmt.Sprintf("step %q output is not JSON-encodable: %v", s.Name, err),
				Code:      codeSerializationError,
				StepID:    s.ID,
				Retryable: false,
			}
		}
	}
	return steps, nil
}

// encodeFailure returns the failure for a result and steps that JSON cannot
// encode, with the steps it can keep; nil when everything encodes.
func encodeFailure(result any, steps []*StepResult) (*PushError, []*StepResult) {
	kept, stepErr := encodableSteps(steps)
	if stepErr != nil {
		return stepErr, kept
	}
	if _, err := json.Marshal(result); err != nil {
		return &PushError{
			Message:   fmt.Sprintf("function result is not JSON-encodable: %v", err),
			Code:      codeSerializationError,
			Retryable: false,
		}, steps
	}
	return nil, steps
}
