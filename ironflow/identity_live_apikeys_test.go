package ironflow

import (
	"errors"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestLiveIdentityAPIKeys covers apikey-crud and apikey-rotate (#3003) on a
// tenant of its own.
func TestLiveIdentityAPIKeys(t *testing.T) {
	tn, ctx, suffix := liveIdentityTenant(t, "apikeys")
	admin := tn.client()

	roles, err := admin.ListRoles(ctx)
	if err != nil {
		t.Fatalf("ListRoles: %v", err)
	}
	var viewer string
	for _, r := range roles {
		if r.Name == "viewer" {
			viewer = r.ID
		}
	}
	if viewer == "" {
		t.Fatalf("no viewer role in %+v", roles)
	}

	name := "go-ci-" + suffix
	created, err := admin.CreateAPIKey(ctx, CreateAPIKeyInput{Name: name, RoleIDs: []string{viewer}, ExpiresIn: "720h"})
	if err != nil {
		t.Fatalf("CreateAPIKey: %v", err)
	}
	if !strings.HasPrefix(created.Key, "ifkey_") || !strings.HasPrefix(created.Key, created.KeyPrefix) ||
		created.Name != name || !slices.Equal(created.RoleIDs, []string{viewer}) || created.ID == "" || created.CreatedAt == "" {
		t.Fatalf("CreateAPIKey = %+v", created)
	}
	if created.ExpiresAt == nil {
		t.Fatalf("expires_in 720h left no expiry: %+v", created)
	}
	if exp, err := time.Parse(time.RFC3339, *created.ExpiresAt); err != nil || time.Until(exp) < 29*24*time.Hour || time.Until(exp) > 31*24*time.Hour {
		t.Errorf("expires_at = %q, want about 30 days out (%v)", *created.ExpiresAt, err)
	}

	t.Run("apikey-crud", func(t *testing.T) {
		got, err := admin.GetAPIKey(ctx, created.ID)
		if err != nil || got.ID != created.ID || got.Name != name || got.KeyPrefix != created.KeyPrefix || !slices.Equal(got.RoleIDs, []string{viewer}) {
			t.Fatalf("GetAPIKey = %+v, %v", got, err)
		}
		list, err := admin.ListAPIKeys(ctx)
		if err != nil {
			t.Fatalf("ListAPIKeys: %v", err)
		}
		if !slices.ContainsFunc(list, func(k APIKeyInfo) bool { return k.ID == created.ID }) {
			t.Errorf("ListAPIKeys %+v misses %s", list, created.ID)
		}
		// The viewer role is what the key holds: it reads, but cannot create keys.
		viewerClient := NewClient(ClientConfig{ServerURL: tn.server, APIKey: created.Key})
		if _, err := viewerClient.GetOrganization(ctx, tn.orgID); err != nil {
			t.Errorf("the new key reads its org: %v", err)
		}
		if _, err := viewerClient.CreateAPIKey(ctx, CreateAPIKeyInput{Name: "denied-" + suffix}); !errors.Is(err, ErrForbidden) {
			t.Errorf("a viewer key creates a key: err = %v, want 403", err)
		}
		if _, err := admin.GetAPIKey(ctx, "ak_does_not_exist"); httpStatusOf(err) != http.StatusNotFound {
			t.Errorf("unknown id: err = %v, want 404", err)
		}
		if _, err := admin.CreateAPIKey(ctx, CreateAPIKeyInput{}); httpStatusOf(err) != http.StatusBadRequest {
			t.Errorf("a key without a name: err = %v, want 400", err)
		}
	})

	var rotated *APIKeyWithSecret
	t.Run("apikey-rotate", func(t *testing.T) {
		var err error
		rotated, err = admin.RotateAPIKey(ctx, created.ID)
		if err != nil {
			t.Fatalf("RotateAPIKey: %v", err)
		}
		if rotated.ID == created.ID || rotated.Key == created.Key || !strings.HasPrefix(rotated.Key, "ifkey_") || !strings.HasPrefix(rotated.Key, rotated.KeyPrefix) {
			t.Fatalf("RotateAPIKey = %+v, want a new id and secret", rotated)
		}
		// Rotation keeps the name, the roles and the expiry.
		if rotated.Name != name || !slices.Equal(rotated.RoleIDs, []string{viewer}) || rotated.ExpiresAt == nil {
			t.Fatalf("RotateAPIKey changed the configuration: %+v, want name %q, roles [%s], an expiry", rotated, name, viewer)
		}
		// Compare the instants, not the strings: the two backends format the stored stamp differently.
		was, _ := time.Parse(time.RFC3339Nano, *created.ExpiresAt)
		now, _ := time.Parse(time.RFC3339Nano, *rotated.ExpiresAt)
		if d := now.Sub(was).Abs(); d >= time.Second {
			t.Errorf("RotateAPIKey moved the expiry from %s to %s", *created.ExpiresAt, *rotated.ExpiresAt)
		}
		if _, err := admin.GetAPIKey(ctx, created.ID); httpStatusOf(err) != http.StatusNotFound {
			t.Errorf("the old key id after rotation: err = %v, want 404", err)
		}
		if _, err := NewClient(ClientConfig{ServerURL: tn.server, APIKey: created.Key}).GetOrganization(ctx, tn.orgID); !errors.Is(err, ErrUnauthorized) {
			t.Errorf("the old secret after rotation: err = %v, want 401", err)
		}
		if _, err := NewClient(ClientConfig{ServerURL: tn.server, APIKey: rotated.Key}).GetOrganization(ctx, tn.orgID); err != nil {
			t.Errorf("the new secret: %v", err)
		}
		if _, err := admin.RotateAPIKey(ctx, "ak_does_not_exist"); httpStatusOf(err) != http.StatusNotFound {
			t.Errorf("rotate an unknown id: err = %v, want 404", err)
		}
	})

	// Delete the rotated key: the id and the secret both stop working.
	t.Run("apikey-crud delete", func(t *testing.T) {
		if rotated == nil {
			t.Fatal("rotation failed; nothing to delete")
		}
		if err := admin.DeleteAPIKey(ctx, rotated.ID); err != nil {
			t.Fatalf("DeleteAPIKey: %v", err)
		}
		if _, err := admin.GetAPIKey(ctx, rotated.ID); httpStatusOf(err) != http.StatusNotFound {
			t.Errorf("Get after Delete: err = %v, want 404", err)
		}
		if _, err := NewClient(ClientConfig{ServerURL: tn.server, APIKey: rotated.Key}).GetOrganization(ctx, tn.orgID); !errors.Is(err, ErrUnauthorized) {
			t.Errorf("the deleted secret: err = %v, want 401", err)
		}
	})

	// A key bound to a second environment with tool scopes: both show on read
	// and list, rotation keeps them, and the binding holds on the wire.
	t.Run("apikey-crud and apikey-rotate keep the environment and tool scopes", func(t *testing.T) {
		projects, err := admin.Projects().List(ctx)
		if err != nil || len(projects) == 0 {
			t.Fatalf("Projects.List = %+v, %v", projects, err)
		}
		staging, err := admin.Projects().CreateEnvironment(ctx, CreateEnvironmentInput{Name: "staging", ProjectID: projects[0].ID})
		if err != nil {
			t.Fatalf("CreateEnvironment: %v", err)
		}
		scopes := []string{"billing-tools", "reporting-tools"}
		scoped, err := admin.CreateAPIKey(ctx, CreateAPIKeyInput{Name: "scoped-" + suffix, EnvID: staging.ID, ToolScopes: scopes})
		if err != nil {
			t.Fatalf("CreateAPIKey: %v", err)
		}
		if scoped.EnvironmentID != staging.ID || !slices.Equal(scoped.ToolScopes, scopes) {
			t.Fatalf("CreateAPIKey = %+v, want environment %s and scopes %v", scoped, staging.ID, scopes)
		}
		bound := func(k APIKeyInfo) bool { return k.EnvironmentID == staging.ID && slices.Equal(k.ToolScopes, scopes) }
		if got, err := admin.GetAPIKey(ctx, scoped.ID); err != nil || !bound(*got) {
			t.Fatalf("GetAPIKey = %+v, %v", got, err)
		}
		list, err := admin.ListAPIKeys(ctx)
		if err != nil || !slices.ContainsFunc(list, func(k APIKeyInfo) bool { return k.ID == scoped.ID && bound(k) }) {
			t.Fatalf("ListAPIKeys %+v, %v: no bound entry for %s", list, err, scoped.ID)
		}

		inEnv := func(key, env string) error {
			_, err := NewClient(ClientConfig{ServerURL: tn.server, APIKey: key, Environment: env}).GetOrganization(ctx, tn.orgID)
			return err
		}
		if err := inEnv(scoped.Key, staging.ID); err != nil {
			t.Errorf("the key in its own environment: %v", err)
		}
		if err := inEnv(scoped.Key, tn.envID); !errors.Is(err, ErrForbidden) {
			t.Errorf("the key in another environment: err = %v, want 403", err)
		}

		rotated, err := admin.RotateAPIKey(ctx, scoped.ID)
		if err != nil || rotated.ID == scoped.ID || !bound(rotated.APIKeyInfo) {
			t.Fatalf("RotateAPIKey = %+v, %v, want a new key still bound to %s with %v", rotated, err, staging.ID, scopes)
		}
		if got, err := admin.GetAPIKey(ctx, rotated.ID); err != nil || !bound(*got) {
			t.Errorf("GetAPIKey after rotation = %+v, %v", got, err)
		}
		if err := inEnv(rotated.Key, staging.ID); err != nil {
			t.Errorf("the rotated key in its environment: %v", err)
		}
		if err := inEnv(rotated.Key, tn.envID); !errors.Is(err, ErrForbidden) {
			t.Errorf("the rotated key in another environment: err = %v, want 403", err)
		}
		if err := admin.DeleteAPIKey(ctx, rotated.ID); err != nil {
			t.Errorf("DeleteAPIKey: %v", err)
		}
	})
}
