package ironflow

import (
	"context"
	"net/url"
	"strconv"
)

// ListAuditEventsOpts configures an environment-wide audit query.
type ListAuditEventsOpts struct {
	RunID         string
	FunctionID    string
	EventType     string
	FromTimestamp string
	ToTimestamp   string
	Limit         int
	Cursor        string
}

// ListAuditEvents queries the environment-wide audit stream.
func (c *Client) ListAuditEvents(ctx context.Context, opts ListAuditEventsOpts) (*AuditTrailResult, error) {
	query := url.Values{}
	if opts.RunID != "" {
		query.Set("run_id", opts.RunID)
	}
	if opts.FunctionID != "" {
		query.Set("function_id", opts.FunctionID)
	}
	if opts.EventType != "" {
		query.Set("event_type", opts.EventType)
	}
	if opts.FromTimestamp != "" {
		query.Set("from", opts.FromTimestamp)
	}
	if opts.ToTimestamp != "" {
		query.Set("to", opts.ToTimestamp)
	}
	if opts.Limit > 0 {
		query.Set("limit", strconv.Itoa(opts.Limit))
	}
	if opts.Cursor != "" {
		query.Set("cursor", opts.Cursor)
	}
	path := "/api/v1/audit"
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	var result AuditTrailResult
	if err := c.request(ctx, "GET", path, nil, &result); err != nil {
		return nil, err
	}
	if result.Events == nil {
		result.Events = []AuditEvent{}
	}
	return &result, nil
}
