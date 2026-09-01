package ironflow

import "context"

// VisibleAgentTool is an agent tool the current API key may invoke.
type VisibleAgentTool struct {
	QualifiedName   string   `json:"qualifiedName"`
	Description     string   `json:"description"`
	InputSchemaJSON string   `json:"inputSchemaJson"`
	RequiredScopes  []string `json:"requiredScopes"`
}

// ListAgentToolsResult contains visible tools and the reserved pagination cursor.
type ListAgentToolsResult struct {
	Tools      []VisibleAgentTool `json:"tools"`
	NextCursor string             `json:"nextCursor"`
}

// ListAgentTools lists agent tools visible to the current API key.
func (c *Client) ListAgentTools(ctx context.Context, cursor string) (*ListAgentToolsResult, error) {
	var result ListAgentToolsResult
	if err := c.request(ctx, "POST", "/ironflow.v1.AgentToolsService/ListTools", map[string]string{"cursor": cursor}, &result); err != nil {
		return nil, err
	}
	if result.Tools == nil {
		result.Tools = []VisibleAgentTool{}
	}
	return &result, nil
}
