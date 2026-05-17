package agent

import "sync"

// RegisteredTool is the SDK-local mirror of an entry in the Ironflow
// server's agent_tools registry. The server holds the canonical record
// (NATS KV in cluster mode, in-memory in single-node). The SDK mirror
// stores the McpToolDef closure (Validate + Handler) and the HMAC
// secret returned by RegisterTool so the dispatch handler can validate
// inbound calls without a server round-trip.
//
// Single instance per Go process. Re-registering the same agent name
// rotates the secret and overwrites prior entries; unregister removes
// them.
type RegisteredTool struct {
	AgentName     string
	QualifiedName string
	HMACSecret    string
	Def           McpToolDef
}

var (
	registryMu sync.RWMutex
	registry   = map[string]RegisteredTool{}
)

// registerLocal stores or replaces an entry keyed by qualified name.
func registerLocal(entry RegisteredTool) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[entry.QualifiedName] = entry
}

// unregisterLocal removes every entry owned by agentName.
func unregisterLocal(agentName string) {
	registryMu.Lock()
	defer registryMu.Unlock()
	for qn, entry := range registry {
		if entry.AgentName == agentName {
			delete(registry, qn)
		}
	}
}

// lookupLocal returns the registered tool for a qualified name, or
// (zero, false) if no entry matches. Caller-safe to copy by value.
func lookupLocal(qualifiedName string) (RegisteredTool, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	entry, ok := registry[qualifiedName]
	return entry, ok
}

// clearLocalForTests drops every entry. Tests only.
func clearLocalForTests() {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry = map[string]RegisteredTool{}
}
