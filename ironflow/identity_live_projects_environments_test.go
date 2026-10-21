package ironflow

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

// TestLiveIdentityProjectsEnvironments covers project-crud and env-crud
// (#3003) on a tenant of its own.
func TestLiveIdentityProjectsEnvironments(t *testing.T) {
	tn, ctx, suffix := liveIdentityTenant(t, "projects")
	projects := tn.client().Projects()

	name := "svc-" + suffix
	var project *Project

	t.Run("project-crud", func(t *testing.T) {
		var err error
		project, err = projects.Create(ctx, CreateProjectInput{Name: name, Description: "created"})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if project.ID == "" || project.Name != name || project.Description != "created" || project.OrgID != tn.orgID || project.CreatedAt == "" {
			t.Fatalf("Create = %+v", project)
		}
		list, err := projects.List(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		var names []string
		for _, p := range list {
			names = append(names, p.Name)
		}
		slices.Sort(names)
		if want := []string{"default", name}; !slices.Equal(names, want) {
			t.Errorf("List names = %v, want %v", names, want)
		}
		if !slices.ContainsFunc(list, func(p Project) bool { return p.ID == project.ID && p.Description == "created" }) {
			t.Errorf("List %+v misses the created description", list)
		}

		renamed := "renamed-" + suffix
		updatedDesc, cleared := "updated", ""
		got, err := projects.Update(ctx, project.ID, UpdateProjectInput{Name: renamed, Description: &updatedDesc})
		// The two stamps can carry different zones, so compare the instants.
		before, _ := time.Parse(time.RFC3339Nano, project.UpdatedAt)
		after, _ := time.Parse(time.RFC3339Nano, got.UpdatedAt)
		if err != nil || got.ID != project.ID || got.Name != renamed || got.Description != "updated" || after.Before(before) || after.IsZero() {
			t.Fatalf("Update = %+v, %v", got, err)
		}
		list, _ = projects.List(ctx)
		if !slices.ContainsFunc(list, func(p Project) bool { return p.ID == project.ID && p.Name == renamed && p.Description == "updated" }) {
			t.Errorf("List after Update %+v misses %s as %q", list, project.ID, renamed)
		}
		// nil keeps the description; a pointer to "" clears it.
		if kept, err := projects.Update(ctx, project.ID, UpdateProjectInput{Name: renamed}); err != nil || kept.Description != "updated" {
			t.Fatalf("Update without a description = %+v, %v; want %q kept", kept, err, "updated")
		}
		if after, err := projects.Update(ctx, project.ID, UpdateProjectInput{Description: &cleared}); err != nil || after.Name != renamed || after.Description != "" {
			t.Fatalf("Update with an empty description = %+v, %v; want it cleared and the name kept", after, err)
		}
		list, _ = projects.List(ctx)
		if !slices.ContainsFunc(list, func(p Project) bool { return p.ID == project.ID && p.Description == "" }) {
			t.Errorf("List after clearing %+v keeps a description on %s", list, project.ID)
		}
		if _, err := projects.Update(ctx, "proj_does_not_exist", UpdateProjectInput{Name: "x"}); httpStatusOf(err) != http.StatusNotFound {
			t.Errorf("Update unknown: err = %v, want 404", err)
		}
		if _, err := projects.Create(ctx, CreateProjectInput{}); httpStatusOf(err) != http.StatusBadRequest {
			t.Errorf("Create without a name: err = %v, want 400", err)
		}
	})

	t.Run("env-crud", func(t *testing.T) {
		if project == nil {
			t.Fatal("project-crud created no project")
		}
		env, err := projects.CreateEnvironment(ctx, CreateEnvironmentInput{Name: "staging", ProjectID: project.ID})
		if err != nil {
			t.Fatalf("CreateEnvironment: %v", err)
		}
		if env.ID == "" || env.Name != "staging" || env.ProjectID != project.ID || env.CreatedAt == "" {
			t.Fatalf("CreateEnvironment = %+v", env)
		}
		if _, err := projects.CreateEnvironment(ctx, CreateEnvironmentInput{Name: "Not Valid!", ProjectID: project.ID}); httpStatusOf(err) != http.StatusBadRequest {
			t.Errorf("an invalid name: err = %v, want 400", err)
		}
		envs, err := projects.ListEnvironments(ctx)
		if err != nil {
			t.Fatalf("ListEnvironments: %v", err)
		}
		if !slices.ContainsFunc(envs, func(e Environment) bool { return e.ID == env.ID && e.ProjectID == project.ID }) ||
			!slices.ContainsFunc(envs, func(e Environment) bool { return e.ID == tn.envID }) {
			t.Errorf("ListEnvironments %+v misses %s or %s", envs, env.ID, tn.envID)
		}

		got, err := projects.UpdateEnvironment(ctx, env.ID, UpdateEnvironmentInput{Name: "qa"})
		if err != nil || got.ID != env.ID || got.Name != "qa" || got.ProjectID != project.ID {
			t.Fatalf("UpdateEnvironment = %+v, %v", got, err)
		}

		// The Connect service sees the same environment.
		code, body := identityPost(t, tn.server+"/ironflow.v1.EnvironmentService/GetEnvironment", tn.key, map[string]string{"id": env.ID})
		var viaConnect struct{ ID, Name string }
		if code != http.StatusOK || json.Unmarshal(body, &viaConnect) != nil || viaConnect.ID != env.ID || viaConnect.Name != "qa" {
			t.Errorf("Connect GetEnvironment = %d %s, want %s named qa", code, body, env.ID)
		}

		if err := projects.DeleteEnvironment(ctx, env.ID); err != nil {
			t.Fatalf("DeleteEnvironment: %v", err)
		}
		envs, _ = projects.ListEnvironments(ctx)
		if slices.ContainsFunc(envs, func(e Environment) bool { return e.ID == env.ID }) {
			t.Errorf("ListEnvironments still has %s after DeleteEnvironment", env.ID)
		}
		if err := projects.DeleteEnvironment(ctx, env.ID); httpStatusOf(err) != http.StatusNotFound {
			t.Errorf("DeleteEnvironment twice: err = %v, want 404", err)
		}
	})

	// The default environment stays, another org cannot see or touch an
	// environment, and the Connect EnvironmentService answers the same way. No
	// SDK wraps that service, so its four methods are posted directly.
	t.Run("env-crud boundaries and the Connect service", func(t *testing.T) {
		if project == nil {
			t.Fatal("project-crud created no project")
		}
		connect := func(method, bearer string, body any) (int, []byte) {
			t.Helper()
			return identityPost(t, tn.server+"/ironflow.v1.EnvironmentService/"+method, bearer, body)
		}

		if err := projects.DeleteEnvironment(ctx, "env_default"); httpStatusOf(err) != http.StatusForbidden || !strings.Contains(err.Error(), "cannot delete default environment") {
			t.Errorf("DeleteEnvironment(env_default): err = %v, want 403 cannot delete default environment", err)
		}
		if code, body := connect("DeleteEnvironment", tn.key, map[string]string{"id": "env_default"}); code != http.StatusForbidden {
			t.Errorf("Connect DeleteEnvironment(env_default) = %d %s, want 403", code, body)
		}

		code, body := connect("CreateEnvironment", tn.key, map[string]string{"name": "via-connect"})
		var created struct{ ID, Name string }
		if code != http.StatusOK || json.Unmarshal(body, &created) != nil || created.ID == "" || created.Name != "via-connect" {
			t.Fatalf("Connect CreateEnvironment = %d %s", code, body)
		}
		var listed struct{ Environments []struct{ ID string } }
		if code, body := connect("ListEnvironments", tn.key, map[string]any{}); code != http.StatusOK ||
			json.Unmarshal(body, &listed) != nil || !slices.ContainsFunc(listed.Environments, func(e struct{ ID string }) bool { return e.ID == created.ID }) {
			t.Errorf("Connect ListEnvironments = %d %s, want %s in it", code, body, created.ID)
		}
		if envs, err := projects.ListEnvironments(ctx); err != nil || !slices.ContainsFunc(envs, func(e Environment) bool { return e.ID == created.ID }) {
			t.Errorf("REST ListEnvironments misses the Connect-created %s: %+v, %v", created.ID, envs, err)
		}

		// Another org: a miss, the same as an environment that does not exist.
		other, _, _ := liveIdentityTenant(t, "projects-other")
		if _, err := other.client().Projects().UpdateEnvironment(ctx, created.ID, UpdateEnvironmentInput{Name: "stolen"}); httpStatusOf(err) != http.StatusNotFound {
			t.Errorf("another org's UpdateEnvironment: err = %v, want 404", err)
		}
		if err := other.client().Projects().DeleteEnvironment(ctx, created.ID); httpStatusOf(err) != http.StatusNotFound {
			t.Errorf("another org's DeleteEnvironment: err = %v, want 404", err)
		}
		for _, method := range []string{"GetEnvironment", "DeleteEnvironment"} {
			if code, body := connect(method, other.key, map[string]string{"id": created.ID}); code != http.StatusNotFound {
				t.Errorf("another org's Connect %s = %d %s, want 404", method, code, body)
			}
		}
		if code, body := connect("GetEnvironment", tn.key, map[string]string{"id": created.ID}); code != http.StatusOK {
			t.Fatalf("the owner lost %s: %d %s", created.ID, code, body)
		}

		if code, body := connect("DeleteEnvironment", tn.key, map[string]string{"id": created.ID}); code != http.StatusOK || !strings.Contains(string(body), "true") {
			t.Errorf("Connect DeleteEnvironment = %d %s, want success", code, body)
		}
		if code, body := connect("GetEnvironment", tn.key, map[string]string{"id": created.ID}); code != http.StatusNotFound {
			t.Errorf("Connect GetEnvironment after delete = %d %s, want 404", code, body)
		}
	})

	t.Run("project-crud delete", func(t *testing.T) {
		if project == nil {
			t.Fatal("project-crud created no project")
		}
		// Deleting a project deletes its environments.
		child, err := projects.CreateEnvironment(ctx, CreateEnvironmentInput{Name: "child", ProjectID: project.ID})
		if err != nil {
			t.Fatalf("CreateEnvironment: %v", err)
		}
		if err := projects.Delete(ctx, project.ID); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		list, _ := projects.List(ctx)
		if slices.ContainsFunc(list, func(p Project) bool { return p.ID == project.ID }) {
			t.Errorf("List still has %s after Delete", project.ID)
		}
		envs, _ := projects.ListEnvironments(ctx)
		if slices.ContainsFunc(envs, func(e Environment) bool { return e.ID == child.ID }) {
			t.Errorf("ListEnvironments still has %s after its project was deleted", child.ID)
		}
		if err := projects.Delete(ctx, project.ID); httpStatusOf(err) != http.StatusNotFound {
			t.Errorf("Delete twice: err = %v, want 404", err)
		}
	})
}
