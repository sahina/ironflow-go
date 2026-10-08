package ironflow

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestConfigConditionalWrites(t *testing.T) {
	type sent struct{ method, ifMatch, ifNoneMatch string }
	var got []sent
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, sent{r.Method, r.Header.Get("If-Match"), r.Header.Get("If-None-Match")})
		if r.Header.Get("If-Match") == "9" {
			w.WriteHeader(http.StatusPreconditionFailed)
			_, _ = w.Write([]byte(`{"error":"config write precondition failed"}`))
			return
		}
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_, _ = w.Write([]byte(`{"name":"app","revision":4}`))
	}))
	defer server.Close()
	cc := (&Client{serverURL: server.URL, httpClient: server.Client(), logger: NewNoopLogger()}).Config()
	ctx := context.Background()
	data := map[string]any{"a": 1}

	if res, err := cc.Create(ctx, "app", data); err != nil || res.Revision != 4 {
		t.Fatalf("Create: %v %v", res, err)
	}
	if _, err := cc.Update(ctx, "app", data, 3); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if _, err := cc.PatchIf(ctx, "app", data, 3); err != nil {
		t.Fatalf("PatchIf: %v", err)
	}
	if err := cc.DeleteIf(ctx, "app", 3); err != nil {
		t.Fatalf("DeleteIf: %v", err)
	}
	if _, err := cc.Set(ctx, "app", data); err != nil {
		t.Fatalf("Set: %v", err)
	}
	want := []sent{{"POST", "", "*"}, {"POST", "3", ""}, {"PATCH", "3", ""}, {"DELETE", "3", ""}, {"POST", "", ""}}
	if len(got) != len(want) {
		t.Fatalf("requests = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("request %d = %v, want %v", i, got[i], want[i])
		}
	}

	_, err := cc.Update(ctx, "app", data, 9)
	var ife *IronflowError
	if !errors.As(err, &ife) || ife.Code != "HTTP_412" {
		t.Fatalf("stale Update error = %v, want code HTTP_412", err)
	}
}
