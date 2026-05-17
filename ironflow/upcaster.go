package ironflow

import (
	"encoding/json"
	"fmt"
)

// UpcasterFunc transforms event data from one version to the next.
type UpcasterFunc func(data json.RawMessage) (json.RawMessage, error)

// UpcasterRegistry manages upcaster functions for event schema migration.
// Upcasters transform event data from an older schema version to a newer one,
// forming a chain: v1 → v2 → v3 (each step applies one upcaster).
// The chain must be complete — if v2→v3 is missing, upcasting from v1→v3 returns an error.
type UpcasterRegistry struct {
	// event_name -> { from_version -> upcaster }
	upcasters      map[string]map[int]UpcasterFunc
	latestVersions map[string]int
}

// NewUpcasterRegistry creates a new empty registry.
func NewUpcasterRegistry() *UpcasterRegistry {
	return &UpcasterRegistry{
		upcasters:      make(map[string]map[int]UpcasterFunc),
		latestVersions: make(map[string]int),
	}
}

// Register adds an upcaster for a specific version transition.
func (r *UpcasterRegistry) Register(eventName string, fromVersion, toVersion int, fn UpcasterFunc) {
	if _, ok := r.upcasters[eventName]; !ok {
		r.upcasters[eventName] = make(map[int]UpcasterFunc)
	}
	r.upcasters[eventName][fromVersion] = fn

	if toVersion > r.latestVersions[eventName] {
		r.latestVersions[eventName] = toVersion
	}
}

// Upcast applies the upcaster chain to transform event data from fromVersion to toVersion.
// Returns an error if the chain is incomplete.
func (r *UpcasterRegistry) Upcast(eventName string, data json.RawMessage, fromVersion, toVersion int) (json.RawMessage, error) {
	if fromVersion >= toVersion {
		return data, nil
	}

	currentData := data
	currentVersion := fromVersion

	for currentVersion < toVersion {
		upcaster, ok := r.upcasters[eventName][currentVersion]
		if !ok {
			return nil, fmt.Errorf(
				"incomplete upcaster chain for %q: no upcaster from v%d to v%d (chain broken at v%d)",
				eventName, fromVersion, toVersion, currentVersion,
			)
		}

		var err error
		currentData, err = upcaster(currentData)
		if err != nil {
			return nil, fmt.Errorf("upcaster %q v%d→v%d failed: %w", eventName, currentVersion, currentVersion+1, err)
		}
		currentVersion++
	}

	return currentData, nil
}

// LatestVersion returns the latest registered version for an event, or 0 if none.
func (r *UpcasterRegistry) LatestVersion(eventName string) int {
	return r.latestVersions[eventName]
}

// UpcastToLatest applies the upcaster chain to transform event data from fromVersion
// to the latest registered version. Returns data unchanged if no upcasters are registered.
func (r *UpcasterRegistry) UpcastToLatest(eventName string, data json.RawMessage, fromVersion int) (json.RawMessage, error) {
	latest := r.latestVersions[eventName]
	if latest == 0 || fromVersion >= latest {
		return data, nil
	}
	return r.Upcast(eventName, data, fromVersion, latest)
}
