package ironflow

import (
	"testing"
)

func TestSecretsReader_Get(t *testing.T) {
	reader := NewSecretsReader(map[string]string{
		"API_KEY":   "sk-123",
		"DB_SECRET": "dbpass",
	})

	var val string
	if err := reader.Get("API_KEY", &val); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != "sk-123" {
		t.Errorf("expected %q, got %q", "sk-123", val)
	}
}

func TestSecretsReader_Get_NotFound(t *testing.T) {
	reader := NewSecretsReader(map[string]string{
		"API_KEY": "sk-123",
	})

	var val string
	err := reader.Get("MISSING_KEY", &val)
	if err == nil {
		t.Fatal("expected error for missing secret")
	}

	if val != "" {
		t.Errorf("expected empty string for dest, got %q", val)
	}
}

func TestSecretsReader_Has(t *testing.T) {
	reader := NewSecretsReader(map[string]string{
		"API_KEY": "sk-123",
	})

	if !reader.Has("API_KEY") {
		t.Error("expected Has to return true for existing key")
	}

	if reader.Has("MISSING_KEY") {
		t.Error("expected Has to return false for missing key")
	}
}

func TestSecretsReader_NilMap(t *testing.T) {
	reader := NewSecretsReader(nil)

	if reader.Has("ANY_KEY") {
		t.Error("expected Has to return false on nil-initialized reader")
	}

	var val string
	err := reader.Get("ANY_KEY", &val)
	if err == nil {
		t.Fatal("expected error for nil-initialized reader")
	}
}

func TestSecretsReader_EmptyMap(t *testing.T) {
	reader := NewSecretsReader(map[string]string{})

	if reader.Has("ANY_KEY") {
		t.Error("expected Has to return false on empty reader")
	}

	var val string
	err := reader.Get("ANY_KEY", &val)
	if err == nil {
		t.Fatal("expected error for empty reader")
	}
}
