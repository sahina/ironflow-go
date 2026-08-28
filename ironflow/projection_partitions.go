package ironflow

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
)

// ListProjectionPartitionsOptions filters projection partition keys.
type ListProjectionPartitionsOptions struct {
	Query string
	Limit int
}

// ListProjectionPartitionsResult contains materialized projection partition keys.
type ListProjectionPartitionsResult struct {
	Partitions []string `json:"partitions"`
	Returned   int      `json:"returned"`
}

// ListProjectionPartitions returns materialized partition keys for a projection.
func (c *Client) ListProjectionPartitions(ctx context.Context, name string, opts ListProjectionPartitionsOptions) (*ListProjectionPartitionsResult, error) {
	query := url.Values{}
	if opts.Query != "" {
		query.Set("q", opts.Query)
	}
	if opts.Limit != 0 {
		query.Set("limit", strconv.Itoa(opts.Limit))
	}
	path := "/api/v1/projections/" + url.PathEscape(name) + "/partitions"
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}

	var result ListProjectionPartitionsResult
	if err := c.request(ctx, http.MethodGet, path, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
