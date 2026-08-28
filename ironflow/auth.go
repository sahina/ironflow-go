package ironflow

import (
	"context"
	"net/url"
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

// CreateOrganization creates a new organization.
func (c *Client) CreateOrganization(ctx context.Context, input CreateOrgInput) (*OrgInfo, error) {
	var result OrgInfo
	if err := c.request(ctx, "POST", "/api/v1/orgs", input, &result); err != nil {
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

// AssignPolicyToRole assigns a policy to a role.
func (c *Client) AssignPolicyToRole(ctx context.Context, roleID, policyID string) error {
	return c.request(ctx, "POST", "/api/v1/roles/"+roleID+"/policies", map[string]string{"policy_id": policyID}, nil)
}

// RemovePolicyFromRole removes a policy assignment from a role.
func (c *Client) RemovePolicyFromRole(ctx context.Context, roleID, policyID string) error {
	return c.request(ctx, "DELETE", "/api/v1/roles/"+roleID+"/policies/"+policyID, nil, nil)
}

// ListRolePolicies lists every policy assigned to a role.
func (c *Client) ListRolePolicies(ctx context.Context, roleID string) ([]PolicyInfo, error) {
	var response struct {
		Policies []PolicyInfo `json:"policies"`
	}
	if err := c.request(ctx, "GET", "/api/v1/roles/"+url.PathEscape(roleID)+"/policies", nil, &response); err != nil {
		return nil, err
	}
	if response.Policies == nil {
		return []PolicyInfo{}, nil
	}
	return response.Policies, nil
}

// ============================================================================
// Policy Management
// ============================================================================

// CreatePolicy creates a new authorization policy.
func (c *Client) CreatePolicy(ctx context.Context, input CreatePolicyInput) (*PolicyInfo, error) {
	var result PolicyInfo
	if err := c.request(ctx, "POST", "/api/v1/policies", input, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ListPolicies lists all policies for the current organization.
func (c *Client) ListPolicies(ctx context.Context) ([]PolicyInfo, error) {
	var result []PolicyInfo
	if err := c.request(ctx, "GET", "/api/v1/policies", nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetPolicy returns a single policy by ID.
func (c *Client) GetPolicy(ctx context.Context, id string) (*PolicyInfo, error) {
	var result PolicyInfo
	if err := c.request(ctx, "GET", "/api/v1/policies/"+id, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// UpdatePolicy updates a policy.
func (c *Client) UpdatePolicy(ctx context.Context, id string, input UpdatePolicyInput) (*PolicyInfo, error) {
	var result PolicyInfo
	if err := c.request(ctx, "PATCH", "/api/v1/policies/"+id, input, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// DeletePolicy deletes a policy.
func (c *Client) DeletePolicy(ctx context.Context, id string) error {
	return c.request(ctx, "DELETE", "/api/v1/policies/"+id, nil, nil)
}
