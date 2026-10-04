// Package discovery fills unset IRONFLOW_* environment variables from the
// engine discovery file (.ironflow/engine.json). See ADR 0098.
package discovery

import (
	"encoding/json"
	"io"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	filePath = ".ironflow/engine.json"
	maxSize  = 64 << 10
)

var once sync.Once

// Hydrate applies the discovery file to the process environment, at most once.
// Call it from a constructor, never at import time, so an application that
// loads its own .env first keeps its values.
func Hydrate() {
	once.Do(func() {
		dir, err := os.Getwd()
		if err != nil {
			return
		}
		for k, v := range Resolve(os.Getenv, dir) {
			_ = os.Setenv(k, v)
		}
	})
}

// Resolve returns the variables to set, or nil when the file does not apply.
// It reads files but touches no global state.
func Resolve(getenv func(string) string, dir string) map[string]string {
	if getenv("IRONFLOW_NO_DISCOVERY") == "1" {
		return nil
	}
	for _, k := range []string{"IRONFLOW_URL", "IRONFLOW_SERVER_URL", "IRONFLOW_API_KEY"} {
		if getenv(k) != "" {
			return nil
		}
	}
	data, ok := findFile(dir)
	if !ok {
		return nil
	}
	var f struct {
		Version     int    `json:"version"`
		URL         string `json:"url"`
		APIKey      string `json:"api_key"` //nolint:gosec // field name, not a credential
		Environment string `json:"environment"`
	}
	if json.Unmarshal(data, &f) != nil || f.Version != 1 || !loopbackHTTP(f.URL) {
		return nil
	}
	out := map[string]string{"IRONFLOW_URL": f.URL, "IRONFLOW_SERVER_URL": f.URL}
	if f.APIKey != "" {
		out["IRONFLOW_API_KEY"] = f.APIKey
	}
	if f.Environment != "" && getenv("IRONFLOW_ENV") == "" {
		out["IRONFLOW_ENV"] = f.Environment
	}
	return out
}

// findFile returns the content of the nearest file, or ok=false if none is
// found or the nearest one is over the size cap (it is not skipped for a
// farther one).
func findFile(dir string) ([]byte, bool) {
	for {
		if f, err := os.Open(filepath.Join(dir, filePath)); err == nil { //nolint:gosec // G304: dir walks up from the caller's working directory by design
			defer func() { _ = f.Close() }()
			data, err := io.ReadAll(io.LimitReader(f, maxSize+1))
			return data, err == nil && len(data) <= maxSize
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return nil, false
		}
		dir = parent
	}
}

func loopbackHTTP(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" {
		return false
	}
	host := u.Hostname()
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip, err := netip.ParseAddr(host)
	return err == nil && ip.IsLoopback()
}
