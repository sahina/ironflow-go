package ironflow

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func setupMockFilesServer(t *testing.T, h http.Handler, attempts int) *Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return &Client{serverURL: srv.URL, httpClient: srv.Client(), apiKey: "k",
		retryConfig: &ClientRetryConfig{MaxAttempts: attempts}, logger: NewNoopLogger()}
}

func TestFilesPutEncodesPathAndHeaders(t *testing.T) {
	var gotPath, gotType, gotMeta, gotINM string
	var gotBody []byte
	c := setupMockFilesServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotType = r.URL.EscapedPath(), r.Header.Get("Content-Type")
		gotMeta, gotINM = r.Header.Get("X-Ironflow-Meta-Source"), r.Header.Get("If-None-Match")
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(201)
		_, _ = w.Write([]byte(`{"bucket":"docs","path":"a b/c.txt","etag":"e1","size":2}`))
	}), 1)
	info, err := c.Files().Bucket("docs").Put(context.Background(), "a b/c.txt", strings.NewReader("hi"), 2,
		PutFileOptions{ContentType: "text/plain", Metadata: map[string]string{"source": "t"}, IfNoneMatch: true})
	if err != nil || info.ETag != "e1" {
		t.Fatalf("%+v %v", info, err)
	}
	if gotPath != "/api/v1/files/buckets/docs/objects/a%20b/c.txt" || gotType != "text/plain" || gotMeta != "t" || gotINM != "*" || string(gotBody) != "hi" {
		t.Fatalf("path=%s type=%s meta=%s inm=%s body=%q", gotPath, gotType, gotMeta, gotINM, gotBody)
	}
}

func TestFilesPutRetriesOnlyReplayableBodies(t *testing.T) {
	var n atomic.Int32
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		if n.Add(1) == 1 {
			w.WriteHeader(503)
			_, _ = w.Write([]byte(`{"code":"INTERNAL","message":"x"}`))
			return
		}
		w.WriteHeader(201)
		_, _ = w.Write([]byte(`{"etag":"e"}`))
	})
	c := setupMockFilesServer(t, h, 3)
	if _, err := c.Files().Bucket("b").Put(context.Background(), "x", bytes.NewReader([]byte("abc")), 3, PutFileOptions{ContentType: "text/plain"}); err != nil {
		t.Fatalf("seekable body not retried: %v", err)
	}
	if n.Load() != 2 {
		t.Fatalf("attempts %d", n.Load())
	}
	n.Store(0)
	_, err := c.Files().Bucket("b").Put(context.Background(), "x", io.MultiReader(strings.NewReader("abc")), 3, PutFileOptions{ContentType: "text/plain"})
	if err == nil {
		t.Fatal("stream body: want the 503 error, not a retry")
	}
	var ife *IronflowError
	if !errors.As(err, &ife) || ife.Code != "INTERNAL" || !strings.Contains(err.Error(), "stream") || !strings.Contains(err.Error(), "replayed") {
		t.Fatalf("want the original error wrapped with a stream message, got %v", err)
	}
	if n.Load() != 1 {
		t.Fatalf("stream body retried: %d attempts", n.Load())
	}
}

func TestFilesPutEmptyFileSendsLengthZero(t *testing.T) {
	c := setupMockFilesServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
			t.Errorf("ContentLength=%d TE=%v", r.ContentLength, r.TransferEncoding)
		}
		w.WriteHeader(201)
		_, _ = w.Write([]byte(`{"etag":"e"}`))
	}), 1)
	if _, err := c.Files().Bucket("b").Put(context.Background(), "empty", bytes.NewReader(nil), 0, PutFileOptions{ContentType: "text/plain"}); err != nil {
		t.Fatal(err)
	}
}

func TestFilesTypedErrors(t *testing.T) {
	for status, want := range map[int]error{412: ErrPreconditionFailed, 413: ErrPayloadTooLarge, 415: ErrUnsupportedMediaType} {
		c := setupMockFilesServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"code":"X","message":"m","error":"m"}`))
		}), 1)
		_, err := c.Files().Bucket("b").Put(context.Background(), "x", strings.NewReader("a"), 1, PutFileOptions{ContentType: "text/plain"})
		if !errors.Is(err, want) {
			t.Errorf("%d: got %v", status, err)
		}
	}
}

func TestFilesGetStreamsAndPinsVersion(t *testing.T) {
	c := setupMockFilesServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-Match") != "e1" {
			t.Errorf("If-Match = %q", r.Header.Get("If-Match"))
		}
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("ETag", `"e1"`)
		_, _ = w.Write([]byte("body"))
	}), 1)
	obj, err := c.Files().Bucket("b").Get(context.Background(), "x", GetFileOptions{IfMatch: "e1"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = obj.Body.Close() }()
	b, _ := io.ReadAll(obj.Body)
	if string(b) != "body" || obj.ETag != "e1" || obj.ContentType != "text/plain" {
		t.Fatalf("%q %+v", b, obj)
	}
}

func TestFilesJSONCalls(t *testing.T) {
	var gotMethod, gotPath, gotBody string
	c := setupMockFilesServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotMethod, gotPath, gotBody = r.Method, r.URL.RequestURI(), string(b)
		_, _ = w.Write([]byte(`{"count":3}`))
	}), 1)
	n, err := c.Files().Bucket("docs").MovePrefix(context.Background(), "notes/", "archive/")
	if err != nil || n != 3 || gotMethod != "POST" || gotPath != "/api/v1/files/buckets/docs/move-prefix" ||
		gotBody != `{"from":"notes/","to":"archive/"}` {
		t.Fatalf("n=%d err=%v %s %s %s", n, err, gotMethod, gotPath, gotBody)
	}
}

func TestFilesByteTransfersIgnoreClientTimeout(t *testing.T) {
	c := setupMockFilesServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		time.Sleep(150 * time.Millisecond)
		if r.Method == http.MethodGet {
			_, _ = w.Write([]byte("late"))
			return
		}
		w.WriteHeader(201)
		_, _ = w.Write([]byte(`{"etag":"e"}`))
	}), 1)
	c.httpClient.Timeout = 50 * time.Millisecond
	ctx := context.Background()
	if _, err := c.Files().Bucket("b").Put(ctx, "x", strings.NewReader("a"), 1, PutFileOptions{ContentType: "text/plain"}); err != nil {
		t.Fatalf("put cut by client timeout: %v", err)
	}
	obj, err := c.Files().Bucket("b").Get(ctx, "x", GetFileOptions{})
	if err != nil {
		t.Fatalf("get cut by client timeout: %v", err)
	}
	defer func() { _ = obj.Body.Close() }()
	if b, _ := io.ReadAll(obj.Body); string(b) != "late" {
		t.Fatalf("body %q", b)
	}
	if c.httpClient.Timeout != 50*time.Millisecond {
		t.Fatal("shared client timeout was mutated")
	}
}

func TestFilesErrorsReturnNilAndNotFoundIsBaseError(t *testing.T) {
	c := setupMockFilesServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		_, _ = w.Write([]byte(`{"code":"NOT_FOUND","message":"no such file"}`))
	}), 1)
	info, err := c.Files().Bucket("b").Info(context.Background(), "gone")
	var ie *IronflowError
	if info != nil || !errors.As(err, &ie) || ie.Code != "NOT_FOUND" {
		t.Fatalf("info=%v err=%v", info, err)
	}
	if obj, err := c.Files().Bucket("b").Get(context.Background(), "gone", GetFileOptions{}); obj != nil || !errors.As(err, &ie) || ie.Code != "NOT_FOUND" {
		t.Fatalf("get obj=%v err=%v", obj, err)
	}
}

func TestFilesBucketCallsAndListQuery(t *testing.T) {
	var gotMethod, gotURI, gotBody, gotEnv string
	c := setupMockFilesServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotMethod, gotURI, gotBody, gotEnv = r.Method, r.URL.RequestURI(), string(b), r.Header.Get(HeaderEnvironment)
		_, _ = w.Write([]byte(`{"files":[{"path":"a"}],"prefixes":["d/"],"nextCursor":"n","buckets":[{"name":"x"}]}`))
	}), 1)
	c.environment = "env_x"
	ctx := context.Background()
	on := true
	if _, err := c.Files().UpdateBucket(ctx, "docs", FileBucketConfig{EmitEvents: &on}); err != nil ||
		gotMethod != "PATCH" || gotURI != "/api/v1/files/buckets/docs" || gotBody != `{"emitEvents":true}` {
		t.Fatalf("update: %v %s %s %s", err, gotMethod, gotURI, gotBody)
	}
	res, err := c.Files().Bucket("docs").List(ctx, ListFilesOptions{Prefix: "a b/", Delimiter: "/", Limit: 5})
	if err != nil || len(res.Files) != 1 || res.NextCursor != "n" || gotURI != "/api/v1/files/buckets/docs/objects?delimiter=%2F&limit=5&prefix=a+b%2F" {
		t.Fatalf("list: %+v %v %s", res, err, gotURI)
	}
	if bs, err := c.Files().ListBuckets(ctx); err != nil || len(bs) != 1 || bs[0].Name != "x" {
		t.Fatalf("buckets: %+v %v", bs, err)
	}
	if gotEnv != "env_x" {
		t.Fatalf("environment header %q", gotEnv)
	}
}

// A "../" segment would be resolved by the HTTP client and reach another bucket.
func TestFilesRejectDotSegmentPaths(t *testing.T) {
	var n atomic.Int32
	c := setupMockFilesServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		w.WriteHeader(200)
	}), 1)
	b := c.Files().Bucket("a")
	ctx := context.Background()
	for _, p := range []string{"../../b/objects/x", "a/./b", "a//b", "/a", "a/", ".."} {
		_, errPut := b.Put(ctx, p, strings.NewReader("x"), 1, PutFileOptions{ContentType: "text/plain"})
		_, errGet := b.Get(ctx, p, GetFileOptions{})
		_, errInfo := b.Info(ctx, p)
		errDel := b.Delete(ctx, p)
		for op, err := range map[string]error{"put": errPut, "get": errGet, "info": errInfo, "delete": errDel} {
			var ife *IronflowError
			if !errors.As(err, &ife) || ife.Code != "VALIDATION" {
				t.Errorf("%s %q: want VALIDATION error, got %v", op, p, err)
			}
		}
	}
	if n.Load() != 0 {
		t.Fatalf("%d requests reached the server", n.Load())
	}
}

// A retried move can fail with "source not found" after the first attempt
// already moved the file.
func TestFilesPostIsNotRetried(t *testing.T) {
	var n atomic.Int32
	c := setupMockFilesServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			w.WriteHeader(503)
			_, _ = w.Write([]byte(`{"code":"INTERNAL","message":"x"}`))
			return
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"etag":"e"}`))
	}), 3)
	if _, err := c.Files().Bucket("b").Move(context.Background(), "a", "b", MoveFileOptions{}); err == nil {
		t.Fatal("want the 503 error, not a retried success")
	}
	if n.Load() != 1 {
		t.Fatalf("move attempted %d times, want 1", n.Load())
	}
}

// A sign call only mints a URL, so a transient failure is retried (#2519).
func TestFilesSignCallsAreRetried(t *testing.T) {
	var n atomic.Int32
	c := setupMockFilesServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1)%2 == 1 {
			w.WriteHeader(503)
			_, _ = w.Write([]byte(`{"code":"INTERNAL","message":"x"}`))
			return
		}
		_, _ = w.Write([]byte(`{"url":"u"}`))
	}), 3)
	b := c.Files().Bucket("b")
	if u, err := b.SignUpload(context.Background(), "x", SignUploadOptions{}); err != nil || u.URL != "u" {
		t.Fatalf("upload: %+v %v", u, err)
	}
	if u, err := b.SignDownload(context.Background(), "x", 0); err != nil || u.URL != "u" {
		t.Fatalf("download: %+v %v", u, err)
	}
	if n.Load() != 4 {
		t.Fatalf("%d requests, want 4", n.Load())
	}
}

// The HTTP client resolves dot segments, so a bucket named ".." would leave
// the bucket route; "/" is still escaped, not split (#2519).
func TestFilesRejectBadBucketNames(t *testing.T) {
	var paths []string
	c := setupMockFilesServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.EscapedPath())
		_, _ = w.Write([]byte(`{}`))
	}), 1)
	ctx := context.Background()
	for _, name := range []string{"", ".", ".."} {
		b := c.Files().Bucket(name)
		_, errInfo := b.Info(ctx, "x")
		_, errList := b.List(ctx, ListFilesOptions{})
		_, errMove := b.Move(ctx, "a", "b", MoveFileOptions{})
		_, errSign := b.SignDownload(ctx, "x", 0)
		_, errGetB := c.Files().GetBucket(ctx, name)
		errDelB := c.Files().DeleteBucket(ctx, name)
		for op, err := range map[string]error{"info": errInfo, "list": errList, "move": errMove, "sign": errSign, "getBucket": errGetB, "deleteBucket": errDelB} {
			var ife *IronflowError
			if !errors.As(err, &ife) || ife.Code != "VALIDATION" {
				t.Errorf("%s %q: want VALIDATION error, got %v", op, name, err)
			}
		}
	}
	if len(paths) != 0 {
		t.Fatalf("requests reached the server: %v", paths)
	}
	if _, err := c.Files().GetBucket(ctx, "a/b"); err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 || paths[0] != "/api/v1/files/buckets/a%2Fb" {
		t.Fatalf("paths %v", paths)
	}
}

// Signed URLs that are off are a bucket setting, not a credential problem.
func TestFilesSignedURLsDisabledHasNoAuthHelp(t *testing.T) {
	c := setupMockFilesServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(403)
		_, _ = w.Write([]byte(`{"code":"SIGNED_URLS_DISABLED","message":"signed URLs disabled"}`))
	}), 1)
	_, err := c.Files().Bucket("b").SignUpload(context.Background(), "x", SignUploadOptions{})
	var ife *IronflowError
	if !errors.As(err, &ife) || ife.Code != "SIGNED_URLS_DISABLED" || !errors.Is(err, ErrForbidden) {
		t.Fatalf("got %v", err)
	}
	if strings.Contains(err.Error(), "IRONFLOW_API_KEY") {
		t.Fatalf("API-key help on a bucket setting: %v", err)
	}
}

// #2512: CreateOnly rides the sign request; unset it is omitted.
func TestFilesSignUploadCreateOnly(t *testing.T) {
	var bodies []string
	c := setupMockFilesServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(b))
		_, _ = w.Write([]byte(`{"url":"u"}`))
	}), 1)
	b := c.Files().Bucket("b")
	if _, err := b.SignUpload(context.Background(), "x", SignUploadOptions{CreateOnly: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.SignUpload(context.Background(), "x", SignUploadOptions{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(bodies[0], `"createOnly":true`) || strings.Contains(bodies[1], "createOnly") {
		t.Fatalf("bodies: %v", bodies)
	}
}
