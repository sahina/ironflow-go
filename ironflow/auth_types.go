package ironflow

import "time"

// ============================================================================
// API Key Types
// ============================================================================

// APIKeyInfo represents an API key without the raw secret.
type APIKeyInfo struct {
	ID         string   `json:"id"`
	Name       string   `json:"name"`
	KeyPrefix  string   `json:"key_prefix"`
	RoleIDs    []string `json:"role_ids,omitempty"`
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
// Policy Types
// ============================================================================

// PolicyInfo represents an authorization policy.
type PolicyInfo struct {
	ID        string    `json:"id"`
	OrgID     string    `json:"org_id"`
	Name      string    `json:"name"`
	Effect    string    `json:"effect"`
	Actions   string    `json:"actions"`
	Resources string    `json:"resources"`
	Condition string    `json:"condition,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// CreatePolicyInput is the input for creating a policy.
type CreatePolicyInput struct {
	Name      string `json:"name"`
	Effect    string `json:"effect"`
	Actions   string `json:"actions"`
	Resources string `json:"resources"`
	Condition string `json:"condition,omitempty"`
	OrgID     string `json:"org_id,omitempty"`
}

// UpdatePolicyInput is the input for updating a policy.
type UpdatePolicyInput struct {
	Name      string `json:"name,omitempty"`
	Effect    string `json:"effect,omitempty"`
	Actions   string `json:"actions,omitempty"`
	Resources string `json:"resources,omitempty"`
	Condition string `json:"condition,omitempty"`
}
