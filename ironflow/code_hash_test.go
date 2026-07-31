package ironflow

import (
	"regexp"
	"testing"
)

var hexHash16 = regexp.MustCompile(`^[0-9a-f]{16}$`)

// The reload signal (#1280) depends on executableHash being a stable content hash:
// a 16-char hex digest, identical across restarts of the same binary (so an
// unchanged restart does not inflate the function version).
func TestExecutableHashIsStableHexDigest(t *testing.T) {
	h := executableHash()
	if h == "" {
		t.Skip("os.Executable unavailable in this environment")
	}
	if !hexHash16.MatchString(h) {
		t.Fatalf("executableHash = %q, want 16 lowercase hex chars", h)
	}
	if h2 := executableHash(); h2 != h {
		t.Fatalf("executableHash not stable across calls: %q vs %q", h, h2)
	}
}
