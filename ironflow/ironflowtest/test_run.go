package ironflowtest

// TestRun holds the results of a test function execution.
type TestRun struct {
	Status           string     // "completed" or "failed"
	Steps            []TestStep // All executed steps in order
	Output           any        // Function return value
	Error            error      // Function error (if failed)
	CompensationsRan []string   // Compensation step names in execution order
}

// StepOutput returns the output of a step by name.
// Returns nil if the step is not found.
func (r *TestRun) StepOutput(name string) any {
	for _, s := range r.Steps {
		if s.Name == name {
			return s.Output
		}
	}
	return nil
}

// TestStep records a single step execution.
type TestStep struct {
	Name   string
	Type   string // "run", "invoke", "sleep", "waitForEvent", "compensate"
	Output any
	Error  error
}
