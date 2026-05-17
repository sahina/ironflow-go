package ironflow

import (
	"context"
	"net/url"
)

// User represents a dashboard user account.
type User struct {
	ID        string   `json:"id"`
	OrgID     string   `json:"org_id"`
	Email     string   `json:"email"`
	Name      string   `json:"name,omitempty"`
	Roles     []string `json:"roles,omitempty"`
	CreatedAt string   `json:"created_at,omitempty"`
	UpdatedAt string   `json:"updated_at,omitempty"`
}

// CreateUserInput contains parameters for creating a new user.
type CreateUserInput struct {
	Email    string   `json:"email"`
	Name     string   `json:"name,omitempty"`
	Password string   `json:"password"`
	Roles    []string `json:"roles,omitempty"`
}

// UpdateUserInput contains parameters for updating an existing user.
type UpdateUserInput struct {
	Name  *string  `json:"name,omitempty"`
	Email *string  `json:"email,omitempty"`
	Roles []string `json:"roles,omitempty"`
}

// Tenant represents a tenant (organization) in the platform.
type Tenant struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	EnvCount  int    `json:"env_count"`
	KeyCount  int    `json:"key_count"`
	CreatedAt string `json:"created_at,omitempty"`
}

// UserClient provides access to the user management API.
type UserClient struct {
	client *Client
}

// Users returns a UserClient for managing users.
func (c *Client) Users() *UserClient {
	return &UserClient{client: c}
}

// Create creates a new user (admin only).
func (uc *UserClient) Create(ctx context.Context, input CreateUserInput) (*User, error) {
	var result User
	if err := uc.client.restRequest(ctx, "POST", "/api/v1/users", input, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// List returns all users in the current organization (admin only).
func (uc *UserClient) List(ctx context.Context) ([]User, error) {
	var result []User
	if err := uc.client.restRequest(ctx, "GET", "/api/v1/users", nil, &result); err != nil {
		return nil, err
	}
	if result == nil {
		return []User{}, nil
	}
	return result, nil
}

// Get returns a single user by ID.
func (uc *UserClient) Get(ctx context.Context, id string) (*User, error) {
	var result User
	if err := uc.client.restRequest(ctx, "GET", "/api/v1/users/"+url.PathEscape(id), nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Update updates a user's profile (admin only).
func (uc *UserClient) Update(ctx context.Context, id string, input UpdateUserInput) (*User, error) {
	var result User
	if err := uc.client.restRequest(ctx, "PATCH", "/api/v1/users/"+url.PathEscape(id), input, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Delete deletes a user (admin only).
func (uc *UserClient) Delete(ctx context.Context, id string) error {
	return uc.client.restRequest(ctx, "DELETE", "/api/v1/users/"+url.PathEscape(id), nil, nil)
}

// TenantClient provides access to the tenant management API (enterprise-only).
type TenantClient struct {
	client *Client
}

// Tenants returns a TenantClient for managing tenants.
func (c *Client) Tenants() *TenantClient {
	return &TenantClient{client: c}
}

// List returns all tenants (enterprise-only).
func (tc *TenantClient) List(ctx context.Context) ([]Tenant, error) {
	var result []Tenant
	if err := tc.client.restRequest(ctx, "GET", "/api/v1/tenants", nil, &result); err != nil {
		return nil, err
	}
	if result == nil {
		return []Tenant{}, nil
	}
	return result, nil
}
