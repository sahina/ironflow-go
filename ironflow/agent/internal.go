package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
)

// stableJSON serializes value with sorted object keys so structurally
// equivalent inputs hash to the same byte sequence regardless of key
// declaration order.
//
// Mirrors the JS stableStringify in @ironflow/node/agent/tool.ts. Maps
// are sorted; struct field order is whatever encoding/json produces
// (deterministic per Go field order). Slices preserve order.
func stableJSON(value any) ([]byte, error) {
	if value == nil {
		return []byte("null"), nil
	}
	canon, err := canonicalize(value)
	if err != nil {
		return nil, err
	}
	return json.Marshal(canon)
}

// canonicalize walks value, sorting map keys and recursing into nested
// values. Anything not a map/slice round-trips through encoding/json
// to leverage MarshalJSON implementations and field-tag handling.
func canonicalize(value any) (any, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case map[string]any:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		ordered := make([]canonicalEntry, 0, len(keys))
		for _, k := range keys {
			child, err := canonicalize(v[k])
			if err != nil {
				return nil, err
			}
			ordered = append(ordered, canonicalEntry{Key: k, Value: child})
		}
		return canonicalMap(ordered), nil
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			child, err := canonicalize(item)
			if err != nil {
				return nil, err
			}
			out[i] = child
		}
		return out, nil
	default:
		// Round-trip through encoding/json to fold structs / typed
		// maps / numeric types into the canonical map[string]any form
		// used above.
		raw, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		var decoded any
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return nil, err
		}
		// If decoded resolved to a map/slice, re-canonicalize so
		// nested keys get sorted.
		switch decoded.(type) {
		case map[string]any, []any:
			return canonicalize(decoded)
		default:
			return decoded, nil
		}
	}
}

// canonicalEntry is a single key/value pair used by canonicalMap.
type canonicalEntry struct {
	Key   string
	Value any
}

// canonicalMap is a slice of pre-sorted entries that marshals as a JSON
// object preserving the slice order. Lets us emit `{a:..,b:..}` shape
// without re-sorting via Go's map randomization.
type canonicalMap []canonicalEntry

// MarshalJSON implements json.Marshaler.
func (m canonicalMap) MarshalJSON() ([]byte, error) {
	var b strings.Builder
	b.WriteByte('{')
	for i, entry := range m {
		if i > 0 {
			b.WriteByte(',')
		}
		key, err := json.Marshal(entry.Key)
		if err != nil {
			return nil, err
		}
		b.Write(key)
		b.WriteByte(':')
		val, err := json.Marshal(entry.Value)
		if err != nil {
			return nil, err
		}
		b.Write(val)
	}
	b.WriteByte('}')
	return []byte(b.String()), nil
}

// hashArgs returns a 16-char hex prefix of the SHA-256 over stable-JSON
// of args. Mirrors the JS hashArgs helper.
func hashArgs(args any) (string, error) {
	bytes, err := stableJSON(args)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(bytes)
	return hex.EncodeToString(sum[:])[:16], nil
}

// escapeMatchValue escapes \ and " for safe interpolation into a
// CEL-style match expression like data.runId == "<value>".
func escapeMatchValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return value
}
