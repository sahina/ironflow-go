package ironflow

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

// identityTenant is a tenant one group's test provisioned for itself, so a
// rename, a delete or a policy-set write cannot reach another SDK's test.
type identityTenant struct {
	server, orgID, envID, key string
}

func (it identityTenant) client() *Client {
	return NewClient(ClientConfig{ServerURL: it.server, APIKey: it.key})
}

// liveIdentityTenant provisions a tenant with the bootstrap key. It skips
// unless IRONFLOW_TEST_PLATFORM_TOKEN is set; `make test-identity-live` sets it.
// The last return value is a suffix that makes names unique.
func liveIdentityTenant(t *testing.T, label string) (identityTenant, context.Context, string) {
	t.Helper()
	if os.Getenv("IRONFLOW_TEST_PLATFORM_TOKEN") == "" {
		t.Skip("IRONFLOW_TEST_PLATFORM_TOKEN not set; run `make test-identity-live`")
	}
	server := os.Getenv("IRONFLOW_TEST_SERVER")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	operator := NewClient(ClientConfig{ServerURL: server, APIKey: os.Getenv("IRONFLOW_TEST_API_KEY")})
	tenant, err := operator.Tenants().Provision(ctx, ProvisionTenantInput{OrgName: "Go " + label + " " + suffix})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	return identityTenant{server: server, orgID: tenant.Org.ID, envID: tenant.Environment.ID, key: tenant.APIKey.Key}, ctx, suffix
}

// httpStatusOf is the HTTP status behind an SDK error, 0 when it has none.
// Every REST path sets Details["http_status"].
func httpStatusOf(err error) int {
	var ie *IronflowError
	if !errors.As(err, &ie) {
		return 0
	}
	if s, ok := ie.Details["http_status"].(int); ok {
		return s
	}
	return 0
}
