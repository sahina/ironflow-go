package ironflow

import "time"

// ============================================================================
// API Key Types
// ============================================================================

// APIKeyInfo represents an API key without the raw secret.
type APIKeyInfo struct {
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	KeyPrefix string   `json:"key_prefix"`
	RoleIDs   []string `json:"role_ids,omitempty"`
	// EnvironmentID is the environment the key is bound to.
	EnvironmentID string `json:"environment_id,omitempty"`
	// ToolScopes limits the agent tools the key may call.
	ToolScopes []string `json:"tool_scopes,omitempty"`
	CreatedAt  string   `json:"created_at"`
	ExpiresAt  *string  `json:"expires_at,omitempty"`
	LastUsedAt *string  `json:"last_used_at,omitempty"`
}

// APIKeyWithSecret includes the raw key (only returned on create/rotate).
type APIKeyWithSecret struct {
	APIKeyInfo
	Key string `json:"key"`
}

// CreateAPIKeyInput is the input for creating an API key.
type CreateAPIKeyInput struct {
	Name      string   `json:"name"`
	EnvID     string   `json:"env_id,omitempty"`
	RoleIDs   []string `json:"role_ids,omitempty"`
	ExpiresIn string   `json:"expires_in,omitempty"`
	// ToolScopes limits the agent tools the key may call. A caller grants only
	// scopes it holds, unless it is an admin.
	ToolScopes []string `json:"tool_scopes,omitempty"`
}

// ============================================================================
// Organization Types
// ============================================================================

// OrgInfo represents an organization.
type OrgInfo struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// CreateOrgInput is the input for creating an organization.
type CreateOrgInput struct {
	Name string `json:"name"`
}

// UpdateOrgInput is the input for updating an organization.
type UpdateOrgInput struct {
	Name string `json:"name,omitempty"`
}

// ============================================================================
// Role Types
// ============================================================================

// RoleInfo represents a role.
type RoleInfo struct {
	ID        string    `json:"id"`
	OrgID     string    `json:"org_id"`
	Name      string    `json:"name"`
	IsDefault bool      `json:"is_default"`
	CreatedAt time.Time `json:"created_at"`
}

// CreateRoleInput is the input for creating a role.
type CreateRoleInput struct {
	Name  string `json:"name"`
	OrgID string `json:"org_id,omitempty"`
}

// UpdateRoleInput is the input for updating a role.
type UpdateRoleInput struct {
	Name string `json:"name,omitempty"`
}

// ============================================================================
// Policy Set Types
// ============================================================================

// Error codes the policy-set routes put in IronflowError.Code. A validation
// failure (400) names the offending files and lines in IronflowError.Message.
const (
	PolicySetErrCompileError      = "policy_set_compile_error"
	PolicySetErrV0Syntax          = "policy_set_v0_syntax"
	PolicySetErrReservedNamespace = "policy_set_reserved_namespace"
	PolicySetErrBuiltinForbidden  = "policy_set_builtin_forbidden"
	PolicySetErrSchemaError       = "policy_set_schema_error"
	PolicySetErrEntrypointType    = "policy_set_entrypoint_type"
	PolicySetErrBadFileName       = "policy_set_bad_file_name"
	PolicySetErrTooLarge          = "policy_set_too_large"
	PolicySetErrCompileTimeout    = "policy_set_compile_timeout"
	PolicySetErrBadMode           = "policy_set_bad_mode"
	PolicySetErrBadDecision       = "policy_set_bad_decision"
	PolicySetErrSprintfFormat     = "policy_set_sprintf_format"
	PolicySetErrBadRequest        = "policy_set_bad_request"           // 400
	PolicySetErrForbidden         = "policy_set_forbidden"             // 403
	PolicySetErrNotFound          = "policy_set_not_found"             // 404
	PolicySetErrVersionConflict   = "policy_set_version_conflict"      // 412
	PolicySetErrIfMatchRequired   = "policy_set_precondition_required" // 428
	PolicySetErrSelfLockout       = "policy_set_self_lockout"          // 422
	PolicySetErrRateLimited       = "policy_set_rate_limited"          // 429, honor RetryAfter
	PolicySetErrBusy              = "policy_set_busy"                  // 429, honor RetryAfter
	// PolicySetErrNotPropagated (503, not retryable): the write committed as the version
	// in the message. To finish propagation send the same request again with
	// ifVersion = that version (a no-op); never resend with the old version.
	PolicySetErrNotPropagated = "policy_set_not_propagated"
)

// PolicySet is an org's Rego policy set.
type PolicySet struct {
	Version   int64             `json:"version"` // 0 when the org has no set
	Modules   map[string]string `json:"modules"`
	Decision  string            `json:"decision,omitempty"`
	Mode      string            `json:"mode"` // "enforce" or "monitor"
	UpdatedAt string            `json:"updated_at,omitempty"`
	UpdatedBy string            `json:"updated_by,omitempty"`
	// EnforcementActive is false when the server runs with policy enforcement
	// off, so an "enforce" set is stored but not applied. Nil from a server
	// that omits it.
	EnforcementActive *bool `json:"enforcement_active,omitempty"`
}

// PolicySetInput is the body of PutPolicySet and ValidatePolicySet.
type PolicySetInput struct {
	Modules map[string]string `json:"modules"`
	// Decision nil keeps the current rule; a pointer to "" clears it.
	Decision *string `json:"decision,omitempty"`
	// Mode omitted keeps the current mode; a new set starts in "monitor".
	Mode string `json:"mode,omitempty"`
}

// PolicySetVersion summarizes one stored version.
type PolicySetVersion struct {
	Version   int64  `json:"version"`
	Mode      string `json:"mode"`
	Decision  string `json:"decision,omitempty"`
	CreatedAt string `json:"created_at"`
	CreatedBy string `json:"created_by,omitempty"`
}

// PolicySetDryRunInput evaluates Input against Set, or against the active set when Set is nil.
type PolicySetDryRunInput struct {
	Set     *PolicySetInput `json:"set,omitempty"`
	Input   map[string]any  `json:"input"`
	Explain bool            `json:"explain,omitempty"`
}

// PolicySetDryRunResult is the set's own verdict, whatever its mode.
type PolicySetDryRunResult struct {
	Decision string   `json:"decision"`
	Mode     string   `json:"mode"`
	Version  int64    `json:"version,omitempty"`
	Reasons  []string `json:"reasons,omitempty"`
	// ReasonsOmitted counts reasons dropped beyond the first 100.
	ReasonsOmitted int    `json:"reasons_omitted,omitempty"`
	Entrypoint     string `json:"entrypoint,omitempty"`
	Error          string `json:"error,omitempty"`
	Explain        string `json:"explain,omitempty"`
}
