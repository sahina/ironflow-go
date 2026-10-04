package discovery

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func write(t *testing.T, dir, content string) {
	t.Helper()
	d := filepath.Join(dir, ".ironflow")
	if err := os.MkdirAll(d, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "engine.json"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

const good = `{"version":1,"url":"http://127.0.0.1:52431","api_key":"ifkey_x","environment":"dev"}`

func TestResolve(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		file string // "" = no file
		want map[string]string
	}{
		{"valid", nil, good, map[string]string{
			"IRONFLOW_URL": "http://127.0.0.1:52431", "IRONFLOW_SERVER_URL": "http://127.0.0.1:52431",
			"IRONFLOW_API_KEY": "ifkey_x", "IRONFLOW_ENV": "dev"}},
		{"api_key optional", nil, `{"version":1,"url":"http://localhost:9123"}`, map[string]string{
			"IRONFLOW_URL": "http://localhost:9123", "IRONFLOW_SERVER_URL": "http://localhost:9123"}},
		{"ipv6 loopback", nil, `{"version":1,"url":"http://[::1]:9123"}`, map[string]string{
			"IRONFLOW_URL": "http://[::1]:9123", "IRONFLOW_SERVER_URL": "http://[::1]:9123"}},
		{"127.0.0.0/8", nil, `{"version":1,"url":"http://127.5.5.5:1"}`, map[string]string{
			"IRONFLOW_URL": "http://127.5.5.5:1", "IRONFLOW_SERVER_URL": "http://127.5.5.5:1"}},
		{"env set when unset only", map[string]string{"IRONFLOW_ENV": "prod"}, good, map[string]string{
			"IRONFLOW_URL": "http://127.0.0.1:52431", "IRONFLOW_SERVER_URL": "http://127.0.0.1:52431",
			"IRONFLOW_API_KEY": "ifkey_x"}},
		{"IRONFLOW_URL set", map[string]string{"IRONFLOW_URL": "http://a"}, good, nil},
		{"IRONFLOW_SERVER_URL set", map[string]string{"IRONFLOW_SERVER_URL": "http://a"}, good, nil},
		{"IRONFLOW_API_KEY set", map[string]string{"IRONFLOW_API_KEY": "k"}, good, nil},
		{"opt-out", map[string]string{"IRONFLOW_NO_DISCOVERY": "1"}, good, nil},
		{"no file", nil, "", nil},
		{"non-loopback", nil, `{"version":1,"url":"http://example.com:9123"}`, nil},
		{"non-loopback ip", nil, `{"version":1,"url":"http://10.0.0.1:9123"}`, nil},
		{"https", nil, `{"version":1,"url":"https://127.0.0.1:9123"}`, nil},
		{"version 2", nil, `{"version":2,"url":"http://127.0.0.1:1"}`, nil},
		{"version missing", nil, `{"url":"http://127.0.0.1:1"}`, nil},
		{"invalid json", nil, `{nope`, nil},
		{"oversize", nil, `{"version":1,"url":"http://127.0.0.1:1","pad":"` + strings.Repeat("x", maxSize) + `"}`, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if tc.file != "" {
				write(t, dir, tc.file)
			}
			got := Resolve(env(tc.env), dir)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for k, v := range tc.want {
				if got[k] != v {
					t.Errorf("%s = %q, want %q", k, got[k], v)
				}
			}
		})
	}
}

func TestResolveNearestParent(t *testing.T) {
	root := t.TempDir()
	mid := filepath.Join(root, "a")
	leaf := filepath.Join(mid, "b", "c")
	if err := os.MkdirAll(leaf, 0o755); err != nil {
		t.Fatal(err)
	}
	write(t, root, `{"version":1,"url":"http://127.0.0.1:1"}`)
	write(t, mid, `{"version":1,"url":"http://127.0.0.1:2"}`)
	if got := Resolve(env(nil), leaf)["IRONFLOW_URL"]; got != "http://127.0.0.1:2" {
		t.Errorf("got %q, want nearest file (port 2)", got)
	}
	// An invalid nearest file hides farther valid ones.
	write(t, mid, `{nope`)
	if got := Resolve(env(nil), leaf); got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

func TestHydrateNeverOverwrites(t *testing.T) {
	dir := t.TempDir()
	write(t, dir, good)
	t.Chdir(dir)
	t.Setenv("IRONFLOW_ENV", "keep")
	t.Setenv("IRONFLOW_URL", "")
	t.Setenv("IRONFLOW_SERVER_URL", "")
	t.Setenv("IRONFLOW_API_KEY", "")
	once = sync.Once{}
	Hydrate()
	if got := os.Getenv("IRONFLOW_URL"); got != "http://127.0.0.1:52431" {
		t.Errorf("IRONFLOW_URL = %q", got)
	}
	if got := os.Getenv("IRONFLOW_ENV"); got != "keep" {
		t.Errorf("IRONFLOW_ENV overwritten: %q", got)
	}
	// Second call is a no-op even though the environment now differs.
	t.Setenv("IRONFLOW_URL", "http://changed")
	Hydrate()
	if got := os.Getenv("IRONFLOW_URL"); got != "http://changed" {
		t.Errorf("second Hydrate ran: %q", got)
	}
}
