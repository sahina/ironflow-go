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
			!reflect.DeepEqual(updated.Metadata, map[string]any{"team": "billing"}) || updated.UpdatedAt.Equal(got.UpdatedAt) || got.CreatedAt.IsZero() {
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

// TestLiveReadinessWebhookExpectedUpdatedAt proves a time.Time survives the trip
// back to the server, so Go needs no exact string field (ADR 0165).
func TestLiveReadinessWebhookExpectedUpdatedAt(t *testing.T) {
	var seed struct{}
	client, ctx := liveReadiness(t, &seed)
	wh := client.Webhooks()

	name := fmt.Sprintf("gosdk-guard-%d", time.Now().UnixNano())
	source, err := wh.CreateSource(ctx, CreateWebhookSourceInput{Name: name, EventPrefix: "gosdk.guard", VerifySecret: "s1", VerifyHeader: "x-sig", VerifyAlgorithm: "hmac-sha256"})
	if err != nil {
		t.Fatalf("CreateSource: %v", err)
	}
	defer func() { _ = wh.DeleteSource(ctx, source.ID) }()

	read, err := wh.GetSource(ctx, source.ID)
	if err != nil {
		t.Fatalf("GetSource: %v", err)
	}
	if read.CreatedAt.IsZero() || read.UpdatedAt.IsZero() {
		t.Fatalf("GetSource = %+v, want CreatedAt and UpdatedAt", read)
	}

	// A value in another zone is the same instant, so the guard must still pass.
	elsewhere := read.UpdatedAt.In(time.FixedZone("elsewhere", 2*60*60))
	current, err := wh.UpdateSource(ctx, UpdateWebhookSourceInput{ID: source.ID, Name: name + "-a", ExpectedUpdatedAt: &elsewhere})
	if err != nil {
		t.Fatalf("UpdateSource with the current value: %v", err)
	}

	stale := read.UpdatedAt
	if _, err := wh.UpdateSource(ctx, UpdateWebhookSourceInput{ID: source.ID, Name: name + "-b", ExpectedUpdatedAt: &stale}); !hasErrorCode(err, "aborted") {
		t.Fatalf("UpdateSource with a stale value = %v, want aborted", err)
	}
	if _, err := wh.RotateSecret(ctx, RotateWebhookSecretInput{ID: source.ID, VerifySecret: "s2", ExpectedUpdatedAt: &stale}); !hasErrorCode(err, "aborted") {
		t.Fatalf("RotateSecret with a stale value = %v, want aborted", err)
	}

	rotated, err := wh.RotateSecret(ctx, RotateWebhookSecretInput{ID: source.ID, VerifySecret: "s2", ExpectedUpdatedAt: &current.UpdatedAt})
	if err != nil {
		t.Fatalf("RotateSecret with the current value: %v", err)
	}
	if rotated.VerifySecretPrevExpiresAt == nil || !rotated.VerifySecretPrevExpiresAt.After(time.Now()) {
		t.Fatalf("RotateSecret = %+v, want a future VerifySecretPrevExpiresAt", rotated)
	}
}

func webhookSourceIndex(sources []WebhookSource, id string) int {
	for i, s := range sources {
		if s.ID == id {
			return i
		}
	}
	return -1
}
