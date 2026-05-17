package ironflow

import (
	"context"
	"encoding/json"
	"net/url"
)

// ProjectClient provides access to the Ironflow Project and Environment Management APIs.
type ProjectClient struct {
	client *Client
}

// Projects returns a ProjectClient for interacting with the project and environment management service.
func (c *Client) Projects() *ProjectClient {
	return &ProjectClient{client: c}
}

// ============================================================================
// Project methods
// ============================================================================

// CreateProjectInput contains the parameters for creating a project.
type CreateProjectInput struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// UpdateProjectInput contains the parameters for updating a project.
type UpdateProjectInput struct {
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
}

// List returns all projects in the current organization.
func (pc *ProjectClient) List(ctx context.Context) ([]Project, error) {
	var raw json.RawMessage
	if err := pc.client.restRequest(ctx, "GET", "/api/v1/projects", nil, &raw); err != nil {
		return nil, err
	}
	// Handle both plain array and wrapped formats.
	var projects []Project
	if err := json.Unmarshal(raw, &projects); err != nil {
		var wrapped struct {
			Projects []Project `json:"projects"`
		}
		if err2 := json.Unmarshal(raw, &wrapped); err2 != nil {
			return nil, err
		}
		projects = wrapped.Projects
	}
	if projects == nil {
		return []Project{}, nil
	}
	return projects, nil
}

// Create creates a new project with the given name and optional description.
func (pc *ProjectClient) Create(ctx context.Context, input CreateProjectInput) (*Project, error) {
	var result Project
	if err := pc.client.restRequest(ctx, "POST", "/api/v1/projects", input, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Update updates an existing project by ID.
func (pc *ProjectClient) Update(ctx context.Context, id string, input UpdateProjectInput) (*Project, error) {
	var result Project
	if err := pc.client.restRequest(ctx, "PUT", "/api/v1/projects/"+url.PathEscape(id), input, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Delete removes a project by ID.
func (pc *ProjectClient) Delete(ctx context.Context, id string) error {
	return pc.client.restRequest(ctx, "DELETE", "/api/v1/projects/"+url.PathEscape(id), nil, nil)
}

// ============================================================================
// Environment methods
// ============================================================================

// CreateEnvironmentInput contains the parameters for creating an environment.
type CreateEnvironmentInput struct {
	Name      string `json:"name"`
	ProjectID string `json:"project_id"`
}

// UpdateEnvironmentInput contains the parameters for updating an environment.
type UpdateEnvironmentInput struct {
	Name string `json:"name,omitempty"`
}

// ListEnvironments returns all environments visible to the current client.
func (pc *ProjectClient) ListEnvironments(ctx context.Context) ([]Environment, error) {
	var raw json.RawMessage
	if err := pc.client.restRequest(ctx, "GET", "/api/v1/environments", nil, &raw); err != nil {
		return nil, err
	}
	var envs []Environment
	if err := json.Unmarshal(raw, &envs); err != nil {
		var wrapped struct {
			Environments []Environment `json:"environments"`
		}
		if err2 := json.Unmarshal(raw, &wrapped); err2 != nil {
			return nil, err
		}
		envs = wrapped.Environments
	}
	if envs == nil {
		return []Environment{}, nil
	}
	return envs, nil
}

// CreateEnvironment creates a new environment within the specified project.
func (pc *ProjectClient) CreateEnvironment(ctx context.Context, input CreateEnvironmentInput) (*Environment, error) {
	var result Environment
	if err := pc.client.restRequest(ctx, "POST", "/api/v1/environments", input, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// UpdateEnvironment updates an existing environment by ID.
func (pc *ProjectClient) UpdateEnvironment(ctx context.Context, id string, input UpdateEnvironmentInput) (*Environment, error) {
	var result Environment
	if err := pc.client.restRequest(ctx, "PUT", "/api/v1/environments/"+url.PathEscape(id), input, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// DeleteEnvironment removes an environment by ID.
func (pc *ProjectClient) DeleteEnvironment(ctx context.Context, id string) error {
	return pc.client.restRequest(ctx, "DELETE", "/api/v1/environments/"+url.PathEscape(id), nil, nil)
}
