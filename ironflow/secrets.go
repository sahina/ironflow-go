package ironflow

import "fmt"

// SecretsReader provides read-only access to resolved secrets.
type SecretsReader struct {
	values map[string]string
}

// NewSecretsReader creates a SecretsReader from a map.
func NewSecretsReader(values map[string]string) SecretsReader {
	if values == nil {
		values = map[string]string{}
	}
	return SecretsReader{values: values}
}

// Get retrieves a secret value by name. Returns error if secret not found.
func (s SecretsReader) Get(name string, dest *string) error {
	v, ok := s.values[name]
	if !ok {
		return fmt.Errorf("secret %q not found", name)
	}
	*dest = v
	return nil
}

// Has checks if a secret exists.
func (s SecretsReader) Has(name string) bool {
	_, ok := s.values[name]
	return ok
}
