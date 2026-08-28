package ironflow

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// StoredEvent is an event persisted in Ironflow's event log.
type StoredEvent struct {
	ID             string    `json:"id"`
	Name           string    `json:"name"`
	Timestamp      time.Time `json:"timestamp"`
	Data           any       `json:"data"`
	Source         string    `json:"source"`
	Metadata       any       `json:"metadata"`
	IdempotencyKey string    `json:"idempotency_key"`
	Processed      bool      `json:"processed"`
	CreatedAt      time.Time `json:"created_at"`
	EntityID       string    `json:"entity_id"`
	EntityType     string    `json:"entity_type"`
	RunID          string    `json:"run_id"`
}

// ListEventsOptions configures event-log filters and keyset pagination.
type ListEventsOptions struct {
	Name    string
	Names   []string
	Sources []string
	Search  string
	Since   *time.Time
	Until   *time.Time
	Limit   int
	Cursor  string
	Before  string
}

// ListEventsResult is one keyset-paginated event-log page.
type ListEventsResult struct {
	Events            []StoredEvent `json:"events"`
	Count             int           `json:"count"`
	Limit             int           `json:"limit"`
	NextCursor        string        `json:"next_cursor"`
	PrevCursor        string        `json:"prev_cursor"`
	HasNext           bool          `json:"has_next"`
	HasPrev           bool          `json:"has_prev"`
	ApproxTotal       *int64        `json:"approx_total"`
	ApproxTotalCapped bool          `json:"approx_total_capped"`
}

// EventNameCount is one event name and its bounded-window count.
type EventNameCount struct {
	Name  string `json:"name"`
	Count int64  `json:"count"`
}

// ListEventNamesOptions filters the event-name facet.
type ListEventNamesOptions struct {
	Sources []string
	Since   *time.Time
	Until   *time.Time
}

// ListEventNamesResult is the bounded event-name facet response.
type ListEventNamesResult struct {
	Names     []EventNameCount `json:"names"`
	Scanned   int64            `json:"scanned"`
	Truncated bool             `json:"truncated"`
	ScanCap   int              `json:"scan_cap"`
}

// ListEvents reads a page from the event log.
func (c *Client) ListEvents(ctx context.Context, opts ListEventsOptions) (*ListEventsResult, error) {
	query := url.Values{}
	if opts.Name != "" {
		query.Set("name", opts.Name)
	}
	if len(opts.Names) != 0 {
		query.Set("names", strings.Join(opts.Names, ","))
	}
	if len(opts.Sources) != 0 {
		query.Set("source", strings.Join(opts.Sources, ","))
	}
	if opts.Search != "" {
		query.Set("search", opts.Search)
	}
	if opts.Cursor != "" {
		query.Set("cursor", opts.Cursor)
	}
	if opts.Before != "" {
		query.Set("before", opts.Before)
	}
	if opts.Limit != 0 {
		query.Set("limit", strconv.Itoa(opts.Limit))
	}
	if opts.Since != nil {
		query.Set("since", opts.Since.Format(time.RFC3339))
	}
	if opts.Until != nil {
		query.Set("until", opts.Until.Format(time.RFC3339))
	}

	var result ListEventsResult
	if err := c.request(ctx, http.MethodGet, "/api/v1/events?"+query.Encode(), nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetEvent returns one persisted event by ID.
func (c *Client) GetEvent(ctx context.Context, eventID string) (*StoredEvent, error) {
	var result StoredEvent
	if err := c.request(ctx, http.MethodGet, "/api/v1/events/"+url.PathEscape(eventID), nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ListEventNames returns event names and counts for filter UIs.
func (c *Client) ListEventNames(ctx context.Context, opts ListEventNamesOptions) (*ListEventNamesResult, error) {
	query := url.Values{}
	if len(opts.Sources) != 0 {
		query.Set("source", strings.Join(opts.Sources, ","))
	}
	if opts.Since != nil {
		query.Set("since", opts.Since.Format(time.RFC3339))
	}
	if opts.Until != nil {
		query.Set("until", opts.Until.Format(time.RFC3339))
	}

	var result ListEventNamesResult
	if err := c.request(ctx, http.MethodGet, "/api/v1/events/names?"+query.Encode(), nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
