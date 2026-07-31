package ironflow

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"sync"
)

// codeHashMetaKey is a reserved function-metadata key carrying a hash of the
// running binary (#1280). The engine bumps a function's VERSION only when its
// registered config changes, and functionsConfigEqual compares metadata — so
// stamping the code hash here is what makes a rebuild-triggered reload observable
// (ironflow_await_reload + the desktop staleness chip gate on that version bump).
// Reserved (`__` prefix) so it can't collide with user metadata.
const codeHashMetaKey = "__ironflow_code_hash"

var (
	execHashOnce sync.Once
	execHashVal  string
)

// executableHash is a short, deterministic hash of the running binary's contents,
// computed once. It changes when the binary is rebuilt (air / go build) and is
// stable across restarts of the same binary — a CONTENT hash, not a nonce, so an
// identical restart does not inflate the function version. Empty string when the
// executable can't be located or read (best-effort: the reload signal degrades to
// off rather than failing registration).
func executableHash() string {
	execHashOnce.Do(func() {
		path, err := os.Executable()
		if err != nil {
			return
		}
		f, err := os.Open(path) //nolint:gosec // G304: path is os.Executable() — the running binary, not user input
		if err != nil {
			return
		}
		defer func() { _ = f.Close() }()
		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			return
		}
		execHashVal = hex.EncodeToString(h.Sum(nil))[:16]
	})
	return execHashVal
}
