package ironflow

import (
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"testing"
)

// TestLiveIdentitySecrets covers secret-crud (#3003) on a tenant of its own, so
// the key is bound to an environment other than env_default. The REST docs say
// a secret's value is write-only, so no response carries it.
func TestLiveIdentitySecrets(t *testing.T) {
	tn, ctx, suffix := liveIdentityTenant(t, "secrets")
	secrets := tn.client().Secrets()

	// The typed Secret has no description or revision, so read them off the wire.
	meta := func(name string) (code int, description string, revision float64) {
		t.Helper()
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, tn.server+"/api/v1/secrets/"+name, nil)
		req.Header.Set("Authorization", "Bearer "+tn.key)
		req.Header.Set("X-Ironflow-Environment", "current")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET secret %s: %v", name, err)
		}
		defer func() { _ = resp.Body.Close() }()
		raw, _ := io.ReadAll(resp.Body)
		var out struct {
			Description string
			Revision    float64
		}
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("GET secret %s: not JSON: %s", name, raw)
		}
		return resp.StatusCode, out.Description, out.Revision
	}

	name := "GO_KEY_" + suffix
	created, err := secrets.Set(ctx, name, "v1-sensitive")
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	if created.Name != name || created.Value != "" || created.CreatedAt == "" || created.UpdatedAt == "" {
		t.Fatalf("Set = %+v, want the metadata and no value", created)
	}
	if got, err := secrets.Get(ctx, name); err != nil || got.Name != name || got.Value != "" {
		t.Fatalf("Get = %+v, %v, want the metadata and no value", got, err)
	}
	code, _, rev := meta(name)
	if code != http.StatusOK || rev < 1 {
		t.Fatalf("metadata after Set = %d, revision %v, want 200 and a revision", code, rev)
	}

	if updated, err := secrets.Update(ctx, name, "v2-sensitive"); err != nil || updated.Name != name || updated.Value != "" {
		t.Fatalf("Update = %+v, %v", updated, err)
	}
	if _, _, after := meta(name); after <= rev {
		t.Errorf("revision after Update = %v, want more than %v", after, rev)
	}

	desc := "billing key"
	if _, err := secrets.Patch(ctx, name, PatchSecretInput{Description: &desc}); err != nil {
		t.Fatalf("Patch description: %v", err)
	}
	if _, got, _ := meta(name); got != desc {
		t.Errorf("description after Patch = %q, want %q", got, desc)
	}
	renamed := "GO_RENAMED_" + suffix
	if got, err := secrets.Patch(ctx, name, PatchSecretInput{Name: &renamed}); err != nil || got.Name != renamed {
		t.Fatalf("Patch rename = %+v, %v", got, err)
	}
	if _, err := secrets.Get(ctx, name); httpStatusOf(err) != http.StatusNotFound {
		t.Errorf("Get under the old name: err = %v, want 404", err)
	}
	if _, got, _ := meta(renamed); got != desc {
		t.Errorf("description after rename = %q, want %q kept", got, desc)
	}
	if _, err := secrets.Patch(ctx, renamed, PatchSecretInput{}); err == nil {
		t.Error("Patch with neither name nor description: err = nil, want a validation error")
	}

	list, err := secrets.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list) != 1 || list[0].Name != renamed {
		t.Errorf("List = %+v, want only %s (a tenant's own environment)", list, renamed)
	}

	if err := secrets.Delete(ctx, renamed); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := secrets.Get(ctx, renamed); httpStatusOf(err) != http.StatusNotFound {
		t.Errorf("Get after Delete: err = %v, want 404", err)
	}
	if list, _ := secrets.List(ctx); slices.ContainsFunc(list, func(e SecretListEntry) bool { return e.Name == renamed }) {
		t.Errorf("List still has %s after Delete", renamed)
	}
}
