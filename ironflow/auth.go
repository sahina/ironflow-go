package ironflow

import (
	"context"
	"errors"
	"strconv"
)

// ============================================================================
// API Key Management
// ============================================================================

// CreateAPIKey creates a new API key.
//
// Example:
//
//	key, err := client.CreateAPIKey(ctx, ironflow.CreateAPIKeyInput{
//	    Name:    "my-key",
//	    RoleIDs: []string{"role_admin"},
//	})
//	fmt.Println("Key:", key.Key) // Only available on create
func (c *Client) CreateAPIKey(ctx context.Context, input CreateAPIKeyInput) (*APIKeyWithSecret, error) {
	var result APIKeyWithSecret
	if err := c.request(ctx, "POST", "/api/v1/apikeys", input, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ListAPIKeys lists all API keys for the current organization.
func (c *Client) ListAPIKeys(ctx context.Context) ([]APIKeyInfo, error) {
	var result []APIKeyInfo
	if err := c.request(ctx, "GET", "/api/v1/apikeys", nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetAPIKey returns a single API key by ID.
func (c *Client) GetAPIKey(ctx context.Context, id string) (*APIKeyInfo, error) {
	var result APIKeyInfo
	if err := c.request(ctx, "GET", "/api/v1/apikeys/"+id, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// DeleteAPIKey deletes an API key.
func (c *Client) DeleteAPIKey(ctx context.Context, id string) error {
	return c.request(ctx, "DELETE", "/api/v1/apikeys/"+id, nil, nil)
}

// RotateAPIKey rotates an API key, returning a new key with the same
// configuration and roles. The old key is deleted.
func (c *Client) RotateAPIKey(ctx context.Context, id string) (*APIKeyWithSecret, error) {
	var result APIKeyWithSecret
	if err := c.request(ctx, "POST", "/api/v1/apikeys/"+id+"/rotate", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ============================================================================
// Organization Management
// ============================================================================

// CreateOrganization creates a new organization. It needs a platform
// credential: a tenant credential cannot create an organization.
func (c *Client) CreateOrganization(ctx context.Context, input CreateOrgInput) (*OrgInfo, error) {
	var result OrgInfo
	// POST /api/v1/orgs always answers 403: tenants cannot create orgs, and a
	// platform credential on a tenant route needs an X-Ironflow-Org header (#2945).
	if err := c.request(ctx, "POST", "/api/v1/platform/orgs", input, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ListOrganizations lists all organizations.
func (c *Client) ListOrganizations(ctx context.Context) ([]OrgInfo, error) {
	var result []OrgInfo
	if err := c.request(ctx, "GET", "/api/v1/orgs", nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetOrganization returns a single organization by ID.
func (c *Client) GetOrganization(ctx context.Context, id string) (*OrgInfo, error) {
	var result OrgInfo
	if err := c.request(ctx, "GET", "/api/v1/orgs/"+id, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// UpdateOrganization updates an organization.
func (c *Client) UpdateOrganization(ctx context.Context, id string, input UpdateOrgInput) (*OrgInfo, error) {
	var result OrgInfo
	if err := c.request(ctx, "PATCH", "/api/v1/orgs/"+id, input, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// DeleteOrganization deletes an organization.
func (c *Client) DeleteOrganization(ctx context.Context, id string) error {
	return c.request(ctx, "DELETE", "/api/v1/orgs/"+id, nil, nil)
}

// ============================================================================
// Role Management
// ============================================================================

// CreateRole creates a new custom role.
func (c *Client) CreateRole(ctx context.Context, input CreateRoleInput) (*RoleInfo, error) {
	var result RoleInfo
	if err := c.request(ctx, "POST", "/api/v1/roles", input, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ListRoles lists all roles for the current organization.
func (c *Client) ListRoles(ctx context.Context) ([]RoleInfo, error) {
	var result []RoleInfo
	if err := c.request(ctx, "GET", "/api/v1/roles", nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetRole returns a single role by ID.
func (c *Client) GetRole(ctx context.Context, id string) (*RoleInfo, error) {
	var result RoleInfo
	if err := c.request(ctx, "GET", "/api/v1/roles/"+id, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// UpdateRole updates a role.
func (c *Client) UpdateRole(ctx context.Context, id string, input UpdateRoleInput) (*RoleInfo, error) {
	var result RoleInfo
	if err := c.request(ctx, "PATCH", "/api/v1/roles/"+id, input, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// DeleteRole deletes a custom role.
func (c *Client) DeleteRole(ctx context.Context, id string) error {
	return c.request(ctx, "DELETE", "/api/v1/roles/"+id, nil, nil)
}

// ============================================================================
// Policy Set
// ============================================================================

// GetPolicySet returns the org's Rego policy set (version 0 when there is none).
func (c *Client) GetPolicySet(ctx context.Context) (*PolicySet, error) {
	var out PolicySet
	if err := c.request(ctx, "GET", "/api/v1/policy-set", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// PutPolicySet replaces the set if its current version is ifVersion (0 for none).
// A 412 means another writer won; IsVersionConflict reports it. A 503
// policy_set_not_propagated is not retryable: the write committed as the version
// named in the message. To finish propagation send the same request again with
// ifVersion set to that version (a no-op); never resend with the old version.
func (c *Client) PutPolicySet(ctx context.Context, in PolicySetInput, ifVersion int64) (*PolicySet, error) {
	var out PolicySet
	if err := c.requestWithHeaders(ctx, "PUT", "/api/v1/policy-set", ifMatch(ifVersion), in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ValidatePolicySet compiles the set without storing it.
func (c *Client) ValidatePolicySet(ctx context.Context, in PolicySetInput) error {
	return c.request(ctx, "POST", "/api/v1/policy-set/validate", in, nil)
}

// DryRunPolicySet evaluates an input against a candidate set or the active one.
func (c *Client) DryRunPolicySet(ctx context.Context, in PolicySetDryRunInput) (*PolicySetDryRunResult, error) {
	var out PolicySetDryRunResult
	if err := c.request(ctx, "POST", "/api/v1/policy-set/dry-run", in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListPolicySetVersions lists the stored versions.
func (c *Client) ListPolicySetVersions(ctx context.Context) ([]PolicySetVersion, error) {
	var out struct {
		Versions []PolicySetVersion `json:"versions"`
	}
	if err := c.request(ctx, "GET", "/api/v1/policy-set/versions", nil, &out); err != nil {
		return nil, err
	}
	return out.Versions, nil
}

// GetPolicySetVersion returns one stored version, modules included.
func (c *Client) GetPolicySetVersion(ctx context.Context, version int64) (*PolicySet, error) {
	var out PolicySet
	if err := c.request(ctx, "GET", "/api/v1/policy-set/versions/"+strconv.FormatInt(version, 10), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// RollbackPolicySet stores a copy of an earlier version's modules as a new
// version, if the current version is ifVersion. The active mode is kept, not
// restored from the old version. A 503 policy_set_not_propagated means the
// rollback committed: send it again with ifVersion set to the saved version
// (a no-op that finishes propagation).
func (c *Client) RollbackPolicySet(ctx context.Context, version, ifVersion int64) (*PolicySet, error) {
	var out PolicySet
	if err := c.requestWithHeaders(ctx, "POST", "/api/v1/policy-set/versions/"+strconv.FormatInt(version, 10)+"/rollback", ifMatch(ifVersion), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// IsVersionConflict reports a 412 from PutPolicySet or RollbackPolicySet.
func IsVersionConflict(err error) bool {
	return errors.Is(err, ErrPreconditionFailed)
}

func ifMatch(version int64) map[string]string {
	return map[string]string{"If-Match": strconv.FormatInt(version, 10)}
}
