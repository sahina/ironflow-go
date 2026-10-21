package ironflow

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

// TestLiveReadinessWebhooks covers the webhooks group (#3003). The secret
// operations and deliveries are in TestLiveReadiness.
func TestLiveReadinessWebhooks(t *testing.T) {
	var seed struct{}
	client, ctx := liveReadiness(t, &seed)
	wh := client.Webhooks()

	t.Run("webhook-source-crud", func(t *testing.T) {
		name := fmt.Sprintf("gosdk-crud-%d", time.Now().UnixNano())
		created, err := wh.CreateSource(ctx, CreateWebhookSourceInput{
			Name: name, EventPrefix: "gosdk.crud", VerifyHeader: "x-sig", VerifyAlgorithm: "hmac-sha256",
			VerifySecret: "s1", Metadata: map[string]any{"team": "payments", "tier": "gold"},
		})
		if err != nil {
			t.Fatalf("CreateSource: %v", err)
		}
		defer func() { _ = wh.DeleteSource(ctx, created.ID) }()
		if !strings.HasPrefix(created.ID, "wh_") || created.Name != name || created.EventPrefix != "gosdk.crud" ||
			created.VerifyHeader != "x-sig" || created.VerifyAlgorithm != "hmac-sha256" || !created.VerifySecretSet ||
			created.IngestToken == "" || !reflect.DeepEqual(created.Metadata, map[string]any{"team": "payments", "tier": "gold"}) {
			t.Fatalf("CreateSource = %+v, want a wh_ ID, the input fields, a secret and the ingest token", created)
		}

		got, err := wh.GetSource(ctx, created.ID)
		if err != nil {
			t.Fatalf("GetSource: %v", err)
		}
		// The token comes back once, on create; every read after that has its prefix only.
		if got.ID != created.ID || got.Name != name || got.EventPrefix != "gosdk.crud" || got.VerifyHeader != "x-sig" ||
			!got.VerifySecretSet || got.IngestToken != "" || got.IngestTokenPrefix == "" || !strings.HasPrefix(created.IngestToken, got.IngestTokenPrefix) {
			t.Fatalf("GetSource = %+v, want the created source without the raw token", got)
		}

		sources, err := wh.ListSources(ctx)
		if err != nil {
			t.Fatalf("ListSources: %v", err)
		}
		if i := webhookSourceIndex(sources, created.ID); i < 0 || sources[i].Name != name {
			t.Fatalf("ListSources = %d sources, none is %s named %s", len(sources), created.ID, name)
		}

		// Name and metadata are full-replace (the "tier" key must go); the header,
		// algorithm and secret are preserve-on-omit, and the event prefix cannot change.
		updated, err := wh.UpdateSource(ctx, UpdateWebhookSourceInput{
			ID: created.ID, Name: name + "-renamed", Metadata: map[string]any{"team": "billing"},
		})
		if err != nil {
			t.Fatalf("UpdateSource: %v", err)
		}
		if updated.Name != name+"-renamed" || updated.EventPrefix != "gosdk.crud" || updated.VerifyHeader != "x-sig" ||
			updated.VerifyAlgorithm != "hmac-sha256" || !updated.VerifySecretSet ||
			!reflect.DeepEqual(updated.Metadata, map[string]any{"team": "billing"}) || updated.UpdatedAt == got.UpdatedAt {
			t.Fatalf("UpdateSource = %+v, want a new name and metadata with the rest kept", updated)
		}
		if again, err := wh.GetSource(ctx, created.ID); err != nil || again.Name != name+"-renamed" {
			t.Fatalf("GetSource after update = %+v, %v, want the new name", again, err)
		}

		if err := wh.DeleteSource(ctx, created.ID); err != nil {
			t.Fatalf("DeleteSource: %v", err)
		}
		_, err = wh.GetSource(ctx, created.ID)
		if !hasErrorCode(err, "not_found") {
			t.Fatalf("GetSource after delete = %v, want not_found", err)
		}
		sources, err = wh.ListSources(ctx)
		if err != nil || webhookSourceIndex(sources, created.ID) >= 0 {
			t.Fatalf("ListSources after delete = %d sources, %v, want none with %s", len(sources), err, created.ID)
		}
	})
}

func webhookSourceIndex(sources []WebhookSource, id string) int {
	for i, s := range sources {
		if s.ID == id {
			return i
		}
	}
	return -1
}
