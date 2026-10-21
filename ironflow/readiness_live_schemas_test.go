package ironflow

import (
	"fmt"
	"reflect"
	"testing"
	"time"
)

// TestLiveReadinessSchemas covers the event-schemas group (#3003) on an event
// of its own, so the shared seed schema (versions 1 and 2, read by
// schema-test-upcast) stays as the seed left it.
func TestLiveReadinessSchemas(t *testing.T) {
	client, ctx := liveReadiness(t, &struct{}{})
	schemas := client.Schemas()
	name := fmt.Sprintf("readiness.schemas.go.%d", time.Now().UnixNano())
	doc := func(field string) map[string]any {
		return map[string]any{"type": "object", "properties": map[string]any{field: map[string]any{"type": "string"}}, "required": []any{field}}
	}

	t.Run("schema-register", func(t *testing.T) {
		for i, field := range []string{"orderId", "orderRef"} {
			v := i + 1
			got, err := schemas.Register(ctx, RegisterSchemaInput{Name: name, Version: v, Schema: doc(field)})
			if err != nil || got.Name != name || got.Version != v || !reflect.DeepEqual(got.Schema, doc(field)) {
				t.Fatalf("Register v%d = %+v, %v, want %s v%d with %v", v, got, err, name, v, doc(field))
			}
		}
		// A document that does not compile is refused at registration.
		if _, err := schemas.Register(ctx, RegisterSchemaInput{Name: name, Version: 3, Schema: map[string]any{"type": "no-such-type"}}); !hasErrorCode(err, "invalid_argument") {
			t.Fatalf("Register of an invalid document = %v, want invalid_argument", err)
		}
	})

	t.Run("schema-read", func(t *testing.T) {
		latest, err := schemas.Get(ctx, name)
		if err != nil || latest.Name != name || latest.Version != 2 || !reflect.DeepEqual(latest.Schema, doc("orderRef")) || latest.CreatedAt == "" {
			t.Fatalf("Get = %+v, %v, want v2 of %s with %v", latest, err, name, doc("orderRef"))
		}
		first, err := schemas.GetVersion(ctx, name, 1)
		if err != nil || first.Version != 1 || !reflect.DeepEqual(first.Schema, doc("orderId")) {
			t.Fatalf("GetVersion 1 = %+v, %v, want v1 with %v", first, err, doc("orderId"))
		}
		all, err := schemas.List(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		var versions []int
		for _, s := range all {
			if s.Name == name {
				versions = append(versions, s.Version)
			}
		}
		if len(versions) != 2 || versions[0]+versions[1] != 3 {
			t.Fatalf("List holds versions %v of %s, want 1 and 2", versions, name)
		}
	})

	t.Run("schema-delete", func(t *testing.T) {
		if err := schemas.Delete(ctx, name, 2); err != nil {
			t.Fatalf("Delete v2: %v", err)
		}
		if latest, err := schemas.Get(ctx, name); err != nil || latest.Version != 1 {
			t.Fatalf("Get after deleting v2 = %+v, %v, want v1", latest, err)
		}
		if _, err := schemas.GetVersion(ctx, name, 2); !hasErrorCode(err, "not_found") {
			t.Fatalf("GetVersion 2 after delete = %v, want not_found", err)
		}
		if err := schemas.Delete(ctx, name, 2); !hasErrorCode(err, "not_found") {
			t.Fatalf("second Delete v2 = %v, want not_found", err)
		}
		all, err := schemas.List(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		for _, s := range all {
			if s.Name == name && s.Version != 1 {
				t.Fatalf("List still holds %+v after deleting v2", s)
			}
		}
		if err := schemas.Delete(ctx, name, 1); err != nil {
			t.Fatalf("Delete v1: %v", err)
		}
		if _, err := schemas.Get(ctx, name); !hasErrorCode(err, "not_found") {
			t.Fatalf("Get after deleting every version = %v, want not_found", err)
		}
	})
}
