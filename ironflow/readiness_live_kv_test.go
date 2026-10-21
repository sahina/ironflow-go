package ironflow

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

// TestLiveReadinessKV covers the kv group (#3003). Each test makes its own
// bucket. Watch is in TestLiveWatch.
func TestLiveReadinessKV(t *testing.T) {
	var seed struct{}
	client, ctx := liveReadiness(t, &seed)
	kv := client.KV()

	t.Run("kv-buckets", func(t *testing.T) {
		name := fmt.Sprintf("gokvb%d", time.Now().UnixNano())
		created, err := kv.CreateBucket(ctx, BucketConfig{Name: name, Description: "readiness", TTL: time.Hour, History: 3})
		if err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}
		defer func() { _ = kv.DeleteBucket(ctx, name) }()
		if created.Name != name || created.Description != "readiness" || created.TTLSeconds != 3600 || created.History != 3 ||
			created.Values != 0 || created.CreatedAt.IsZero() {
			t.Fatalf("CreateBucket = %+v, want the config with no values", created)
		}

		if _, err := kv.Bucket(name).Put(ctx, "k", []byte(`{"v":1}`)); err != nil {
			t.Fatalf("Put: %v", err)
		}
		info, err := kv.GetBucketInfo(ctx, name)
		if err != nil {
			t.Fatalf("GetBucketInfo: %v", err)
		}
		if info.Name != name || info.Description != "readiness" || info.TTLSeconds != 3600 || info.History != 3 ||
			info.Values != 1 || info.Bytes == 0 || !info.CreatedAt.Equal(created.CreatedAt) {
			t.Fatalf("GetBucketInfo = %+v, want the config with the one value", info)
		}

		buckets, err := kv.ListBuckets(ctx)
		if err != nil {
			t.Fatalf("ListBuckets: %v", err)
		}
		if i := slices.IndexFunc(buckets, func(b BucketInfo) bool { return b.Name == name }); i < 0 || buckets[i].Values != 1 {
			t.Fatalf("ListBuckets = %d buckets, none is %s with one value", len(buckets), name)
		}

		if err := kv.DeleteBucket(ctx, name); err != nil {
			t.Fatalf("DeleteBucket: %v", err)
		}
		if _, err := kv.GetBucketInfo(ctx, name); !hasErrorCode(err, "not_found") {
			t.Fatalf("GetBucketInfo after delete = %v, want not_found", err)
		}
		buckets, err = kv.ListBuckets(ctx)
		if err != nil || slices.IndexFunc(buckets, func(b BucketInfo) bool { return b.Name == name }) >= 0 {
			t.Fatalf("ListBuckets after delete still has %s: %v", name, err)
		}
	})

	t.Run("kv-keys", func(t *testing.T) {
		name := fmt.Sprintf("gokvk%d", time.Now().UnixNano())
		if _, err := kv.CreateBucket(ctx, BucketConfig{Name: name}); err != nil {
			t.Fatalf("CreateBucket: %v", err)
		}
		defer func() { _ = kv.DeleteBucket(ctx, name) }()
		bucket := kv.Bucket(name)

		// The non-ASCII value proves the bytes survive the base64 round trip.
		value := []byte(`{"who":"José"}`)
		rev, err := bucket.Put(ctx, "user.a", value)
		if err != nil || rev == 0 {
			t.Fatalf("Put = %d, %v", rev, err)
		}
		got, err := bucket.Get(ctx, "user.a")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.Key != "user.a" || string(got.Value) != string(value) || got.Revision != rev || got.Operation != "put" || got.CreatedAt == "" {
			t.Fatalf("Get = %+v, want user.a put %s at revision %d", got, value, rev)
		}

		// Create refuses an existing key; Update is a compare-and-swap on the revision.
		if _, err := bucket.Create(ctx, "user.a", []byte(`1`)); !hasErrorCode(err, "failed_precondition") {
			t.Fatalf("Create of an existing key = %v, want failed_precondition", err)
		}
		next, err := bucket.Update(ctx, "user.a", []byte(`2`), rev)
		if err != nil || next <= rev {
			t.Fatalf("Update = %d, %v, want a revision after %d", next, err, rev)
		}
		if _, err := bucket.Update(ctx, "user.a", []byte(`3`), rev); !hasErrorCode(err, "failed_precondition") {
			t.Fatalf("Update at a stale revision = %v, want failed_precondition", err)
		}
		if got, err := bucket.Get(ctx, "user.a"); err != nil || string(got.Value) != "2" || got.Revision != next {
			t.Fatalf("Get after update = %+v, %v, want 2 at revision %d", got, err, next)
		}

		for _, key := range []string{"user.b", "other.c"} {
			if _, err := bucket.Create(ctx, key, []byte(`{}`)); err != nil {
				t.Fatalf("Create %s: %v", key, err)
			}
		}
		all, err := bucket.ListKeys(ctx, "")
		slices.Sort(all)
		if err != nil || !slices.Equal(all, []string{"other.c", "user.a", "user.b"}) {
			t.Fatalf("ListKeys = %v, %v, want all three keys", all, err)
		}
		users, err := bucket.ListKeys(ctx, "user.*")
		slices.Sort(users)
		if err != nil || !slices.Equal(users, []string{"user.a", "user.b"}) {
			t.Fatalf("ListKeys user.* = %v, %v, want the two user keys", users, err)
		}

		// Delete leaves a tombstone: the key reads as missing and drops out of the list.
		if err := bucket.Delete(ctx, "user.a"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if _, err := bucket.Get(ctx, "user.a"); !hasErrorCode(err, "not_found") {
			t.Fatalf("Get after delete = %v, want not_found", err)
		}
		if err := bucket.Purge(ctx, "user.b"); err != nil {
			t.Fatalf("Purge: %v", err)
		}
		left, err := bucket.ListKeys(ctx, "")
		if err != nil || !slices.Equal(left, []string{"other.c"}) {
			t.Fatalf("ListKeys after delete and purge = %v, %v, want only other.c", left, err)
		}
	})
}
