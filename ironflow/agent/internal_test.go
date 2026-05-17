package agent

import (
	"strings"
	"testing"
)

func TestStableJSON_SortsKeys(t *testing.T) {
	a, err := stableJSON(map[string]any{"b": 2, "a": 1})
	if err != nil {
		t.Fatalf("stableJSON a: %v", err)
	}
	b, err := stableJSON(map[string]any{"a": 1, "b": 2})
	if err != nil {
		t.Fatalf("stableJSON b: %v", err)
	}
	if string(a) != string(b) {
		t.Fatalf("stableJSON not stable across key order: %q vs %q", a, b)
	}
	if !strings.Contains(string(a), `"a":1`) || !strings.Contains(string(a), `"b":2`) {
		t.Fatalf("stableJSON missing keys: %q", a)
	}
}

func TestStableJSON_RecursesNested(t *testing.T) {
	a, err := stableJSON(map[string]any{
		"outer": map[string]any{"z": 1, "a": 2},
	})
	if err != nil {
		t.Fatalf("stableJSON: %v", err)
	}
	got := string(a)
	if !strings.Contains(got, `"outer":{"a":2,"z":1}`) {
		t.Fatalf("nested keys not sorted: %q", got)
	}
}

func TestStableJSON_NilSafe(t *testing.T) {
	out, err := stableJSON(nil)
	if err != nil {
		t.Fatalf("stableJSON nil: %v", err)
	}
	if string(out) != "null" {
		t.Fatalf("nil expected null, got %q", out)
	}
}

func TestHashArgs_Deterministic(t *testing.T) {
	a, err := hashArgs(map[string]any{"x": 1, "y": "hello"})
	if err != nil {
		t.Fatalf("hashArgs a: %v", err)
	}
	b, err := hashArgs(map[string]any{"y": "hello", "x": 1})
	if err != nil {
		t.Fatalf("hashArgs b: %v", err)
	}
	if a != b {
		t.Fatalf("hash differs across key order: %q vs %q", a, b)
	}
	if len(a) != 16 {
		t.Fatalf("hash length expected 16 hex chars, got %d (%q)", len(a), a)
	}
}

func TestHashArgs_DifferentInputsDiffer(t *testing.T) {
	a, _ := hashArgs(map[string]any{"x": 1})
	b, _ := hashArgs(map[string]any{"x": 2})
	if a == b {
		t.Fatalf("distinct inputs collided: %q == %q", a, b)
	}
}

func TestEscapeMatchValue(t *testing.T) {
	cases := map[string]string{
		"plain":          "plain",
		`with "quotes"`:  `with \"quotes\"`,
		`back\slash`:     `back\\slash`,
		`mix"\and"`:      `mix\"\\and\"`,
		`run-id-123-abc`: `run-id-123-abc`,
	}
	for in, want := range cases {
		if got := escapeMatchValue(in); got != want {
			t.Errorf("escape(%q) = %q, want %q", in, got, want)
		}
	}
}
