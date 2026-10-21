package ironflow

import (
	"bytes"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"
	"testing"
)

const (
	liveDenyDelete = "package ironflow.authz\n\ndeny contains \"no deletes\" if input.request.action == \"runs:delete\"\n"
	liveDenyCancel = "package ironflow.authz\n\ndeny contains \"no cancels\" if input.request.action == \"runs:cancel\"\n"
)

// TestLiveIdentityRoles covers role-crud (#3003) on a tenant of its own.
func TestLiveIdentityRoles(t *testing.T) {
	tn, ctx, suffix := liveIdentityTenant(t, "roles")
	admin := tn.client()

	name := "deployer-" + suffix
	created, err := admin.CreateRole(ctx, CreateRoleInput{Name: name})
	if err != nil {
		t.Fatalf("CreateRole: %v", err)
	}
	if created.ID == "" || created.Name != name || created.OrgID != tn.orgID || created.IsDefault || created.CreatedAt.IsZero() {
		t.Fatalf("CreateRole = %+v", created)
	}
	if _, err := admin.CreateRole(ctx, CreateRoleInput{Name: name}); httpStatusOf(err) != http.StatusConflict {
		t.Errorf("a duplicate name: err = %v, want 409", err)
	}
	if _, err := admin.CreateRole(ctx, CreateRoleInput{Name: "admin"}); httpStatusOf(err) != http.StatusBadRequest {
		t.Errorf("a built-in name: err = %v, want 400", err)
	}

	// Equal compares the instant: PostgreSQL answers a create in UTC and a read in local time.
	if got, err := admin.GetRole(ctx, created.ID); err != nil || got.ID != created.ID || got.Name != name || got.OrgID != tn.orgID || got.IsDefault || !got.CreatedAt.Equal(created.CreatedAt) {
		t.Fatalf("GetRole = %+v, %v, want %+v", got, err, created)
	}
	list, err := admin.ListRoles(ctx)
	if err != nil {
		t.Fatalf("ListRoles: %v", err)
	}
	var names []string
	builtin := ""
	for _, r := range list {
		names = append(names, r.Name)
		if r.Name == "admin" {
			builtin = r.ID
		}
	}
	slices.Sort(names)
	want := []string{"admin", "developer", name, "viewer"}
	slices.Sort(want)
	if !slices.Equal(names, want) {
		t.Errorf("ListRoles names = %v, want %v", names, want)
	}

	renamed := "releaser-" + suffix
	if got, err := admin.UpdateRole(ctx, created.ID, UpdateRoleInput{Name: renamed}); err != nil || got.Name != renamed || got.ID != created.ID {
		t.Fatalf("UpdateRole = %+v, %v", got, err)
	}
	if got, err := admin.GetRole(ctx, created.ID); err != nil || got.Name != renamed {
		t.Fatalf("GetRole after UpdateRole = %+v, %v", got, err)
	}
	// Built-in roles are fixed.
	if _, err := admin.UpdateRole(ctx, builtin, UpdateRoleInput{Name: "root"}); httpStatusOf(err) != http.StatusBadRequest {
		t.Errorf("rename a built-in role: err = %v, want 400", err)
	}
	if err := admin.DeleteRole(ctx, builtin); httpStatusOf(err) != http.StatusBadRequest {
		t.Errorf("delete a built-in role: err = %v, want 400", err)
	}

	if err := admin.DeleteRole(ctx, created.ID); err != nil {
		t.Fatalf("DeleteRole: %v", err)
	}
	if _, err := admin.GetRole(ctx, created.ID); httpStatusOf(err) != http.StatusNotFound {
		t.Errorf("GetRole after DeleteRole: err = %v, want 404", err)
	}
	if err := admin.DeleteRole(ctx, created.ID); httpStatusOf(err) != http.StatusNotFound {
		t.Errorf("DeleteRole twice: err = %v, want 404", err)
	}
}

// TestLiveIdentityPolicySet covers policy-set (#3003) on a tenant of its own.
// The sets deny one action in monitor mode, so they change nothing the other
// tests do.
func TestLiveIdentityPolicySet(t *testing.T) {
	tn, ctx, _ := liveIdentityTenant(t, "policyset")
	admin := tn.client()
	v1 := PolicySetInput{Modules: map[string]string{"authz.rego": liveDenyDelete}, Mode: "monitor"}
	v2 := PolicySetInput{Modules: map[string]string{"authz.rego": liveDenyDelete, "more.rego": liveDenyCancel}, Mode: "monitor"}
	deleteInput := map[string]any{"request": map[string]any{"action": "runs:delete"}, "subject": map[string]any{}}
	cancelInput := map[string]any{"request": map[string]any{"action": "runs:cancel"}, "subject": map[string]any{}}

	empty, err := admin.GetPolicySet(ctx)
	if err != nil || empty.Version != 0 || len(empty.Modules) != 0 || empty.EnforcementActive == nil {
		t.Fatalf("GetPolicySet on a new org = %+v, %v, want version 0, no files, enforcement_active present", empty, err)
	}

	// validate and dry-run store nothing.
	if err := admin.ValidatePolicySet(ctx, v1); err != nil {
		t.Fatalf("ValidatePolicySet: %v", err)
	}
	bad := PolicySetInput{Modules: map[string]string{"bad.rego": "package ironflow.authz\n\ndeny contains {\n"}}
	if err := admin.ValidatePolicySet(ctx, bad); !hasErrorCode(err, PolicySetErrCompileError) || httpStatusOf(err) != http.StatusBadRequest {
		t.Errorf("a set that does not compile: err = %v, want 400 %s", err, PolicySetErrCompileError)
	}
	dry, err := admin.DryRunPolicySet(ctx, PolicySetDryRunInput{Set: &v1, Input: deleteInput})
	if err != nil || dry.Decision != "deny" || !slices.Contains(dry.Reasons, "no deletes") {
		t.Fatalf("DryRunPolicySet deny = %+v, %v", dry, err)
	}
	if dry, err := admin.DryRunPolicySet(ctx, PolicySetDryRunInput{Set: &v1, Input: cancelInput}); err != nil || dry.Decision != "allow" {
		t.Fatalf("DryRunPolicySet allow = %+v, %v", dry, err)
	}
	if got, _ := admin.GetPolicySet(ctx); got.Version != 0 {
		t.Fatalf("validate and dry-run stored version %d", got.Version)
	}

	put1, err := admin.PutPolicySet(ctx, v1, 0)
	if err != nil || put1.Version != 1 || put1.Mode != "monitor" || !maps.Equal(put1.Modules, v1.Modules) || put1.UpdatedAt == "" {
		t.Fatalf("PutPolicySet v1 = %+v, %v", put1, err)
	}
	if _, err := admin.PutPolicySet(ctx, v2, 0); !IsVersionConflict(err) {
		t.Errorf("a stale If-Match: err = %v, want 412", err)
	}
	put2, err := admin.PutPolicySet(ctx, v2, 1)
	if err != nil || put2.Version != 2 || !maps.Equal(put2.Modules, v2.Modules) {
		t.Fatalf("PutPolicySet v2 = %+v, %v", put2, err)
	}
	if got, err := admin.GetPolicySet(ctx); err != nil || got.Version != 2 || !maps.Equal(got.Modules, v2.Modules) {
		t.Fatalf("GetPolicySet = %+v, %v", got, err)
	}

	versions, err := admin.ListPolicySetVersions(ctx)
	if err != nil {
		t.Fatalf("ListPolicySetVersions: %v", err)
	}
	var seen []int64
	for _, v := range versions {
		seen = append(seen, v.Version)
		if v.Mode != "monitor" || v.CreatedAt == "" {
			t.Errorf("version summary = %+v", v)
		}
	}
	slices.Sort(seen)
	if !slices.Equal(seen, []int64{1, 2}) {
		t.Errorf("ListPolicySetVersions = %v, want 1 and 2", seen)
	}
	if got, err := admin.GetPolicySetVersion(ctx, 1); err != nil || got.Version != 1 || !maps.Equal(got.Modules, v1.Modules) {
		t.Fatalf("GetPolicySetVersion(1) = %+v, %v", got, err)
	}
	if _, err := admin.GetPolicySetVersion(ctx, 99); httpStatusOf(err) != http.StatusNotFound {
		t.Errorf("an unknown version: err = %v, want 404", err)
	}

	// Rollback stores version 1's files as version 3 and keeps the mode.
	rolled, err := admin.RollbackPolicySet(ctx, 1, 2)
	if err != nil || rolled.Version != 3 || rolled.Mode != "monitor" || !maps.Equal(rolled.Modules, v1.Modules) {
		t.Fatalf("RollbackPolicySet = %+v, %v", rolled, err)
	}
	if _, err := admin.RollbackPolicySet(ctx, 1, 2); !IsVersionConflict(err) {
		t.Errorf("rollback with a stale If-Match: err = %v, want 412", err)
	}
	// Without a candidate set, dry-run evaluates the active one.
	active, err := admin.DryRunPolicySet(ctx, PolicySetDryRunInput{Input: cancelInput})
	if err != nil || active.Decision != "allow" || active.Version != 3 {
		t.Fatalf("DryRunPolicySet on the active set = %+v, %v, want allow at version 3", active, err)
	}
}

// TestLiveIdentityPolicySetWrites covers the two write answers the SDK cannot
// reach: a PUT without If-Match is 428, and a PUT equal to the active set is
// 200 with the same version and no new version. Each org may write 5 times in
// a burst (a write earns back one token every 6 seconds, and the budget is
// checked before If-Match), and TestLiveIdentityPolicySet already spends its
// own, so this runs on a second tenant.
func TestLiveIdentityPolicySetWrites(t *testing.T) {
	tn, ctx, _ := liveIdentityTenant(t, "policywrites")
	admin := tn.client()
	set := PolicySetInput{Modules: map[string]string{"authz.rego": liveDenyDelete}, Mode: "monitor"}

	first, err := admin.PutPolicySet(ctx, set, 0)
	if err != nil || first.Version != 1 {
		t.Fatalf("PutPolicySet = %+v, %v", first, err)
	}
	same, err := admin.PutPolicySet(ctx, set, 1)
	if err != nil || same.Version != 1 {
		t.Fatalf("PutPolicySet with the active set = %+v, %v, want 200 at version 1", same, err)
	}
	if versions, err := admin.ListPolicySetVersions(ctx); err != nil || len(versions) != 1 {
		t.Errorf("versions after an unchanged PUT = %+v, %v, want one", versions, err)
	}

	body, _ := json.Marshal(set)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, tn.server+"/api/v1/policy-set", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+tn.key)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusPreconditionRequired || !strings.Contains(string(raw), PolicySetErrIfMatchRequired) {
		t.Errorf("PUT without If-Match = %d %s, want 428 %s", resp.StatusCode, raw, PolicySetErrIfMatchRequired)
	}
}
