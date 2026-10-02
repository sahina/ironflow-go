package ironflow

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// FileBucketConfig holds the settable bucket fields. A nil field is omitted:
// on create the server default applies, on update the stored value stays.
type FileBucketConfig struct {
	MaxObjectBytes      *int64    `json:"maxObjectBytes,omitempty"`
	AllowedContentTypes *[]string `json:"allowedContentTypes,omitempty"`
	EmitEvents          *bool     `json:"emitEvents,omitempty"`
	AllowSignedURLs     *bool     `json:"allowSignedUrls,omitempty"`
}

// FileBucketInfo is a file bucket as the server returns it.
type FileBucketInfo struct {
	Name                string    `json:"name"`
	MaxObjectBytes      int64     `json:"maxObjectBytes"`
	AllowedContentTypes []string  `json:"allowedContentTypes"`
	EmitEvents          bool      `json:"emitEvents"`
	AllowSignedURLs     bool      `json:"allowSignedUrls"`
	CreatedAt           time.Time `json:"createdAt"`
	UpdatedAt           time.Time `json:"updatedAt"`
}

// FileInfo describes one stored file. ETag changes on every write.
type FileInfo struct {
	Bucket      string            `json:"bucket"`
	Path        string            `json:"path"`
	Size        int64             `json:"size"`
	ContentType string            `json:"contentType"`
	SHA256      string            `json:"sha256"`
	ETag        string            `json:"etag"`
	Metadata    map[string]string `json:"metadata"`
	CreatedAt   time.Time         `json:"createdAt"`
	UpdatedAt   time.Time         `json:"updatedAt"`
}

// ListFilesOptions filters a file listing. With Delimiter "/" the result's
// Prefixes holds one level of "folders".
type ListFilesOptions struct {
	Prefix, Delimiter, Cursor string
	Limit                     int
}

// ListFilesResult is one page of a listing; NextCursor is empty on the last.
type ListFilesResult struct {
	Files      []FileInfo `json:"files"`
	Prefixes   []string   `json:"prefixes"`
	NextCursor string     `json:"nextCursor"`
}

// PutFileOptions controls an upload. ContentType is required.
type PutFileOptions struct {
	ContentType string
	Metadata    map[string]string
	// IfMatch overwrites only the version with this ETag.
	IfMatch string
	// IfNoneMatch creates the file only if the path is free.
	IfNoneMatch bool
}

// GetFileOptions controls a download. IfMatch pins the version to read.
type GetFileOptions struct{ IfMatch, Range string }

// FileObject is a streamed download; the caller closes Body.
type FileObject struct {
	Body        io.ReadCloser
	ContentType string
	ETag        string
	Size        int64 // -1 when the server sent no Content-Length
}

// MoveFileOptions controls a single-file move.
type MoveFileOptions struct{ ToBucket, IfMatch string }

// CopyFileOptions controls a single-file copy.
type CopyFileOptions struct{ ToBucket string }

// SignUploadOptions limits what a signed upload URL accepts.
type SignUploadOptions struct {
	TTL         time.Duration
	MaxBytes    int64
	ContentType string
	// CreateOnly makes the PUT fail with 412 when the path already exists, so
	// the URL cannot overwrite a file.
	CreateOnly bool
}

// SignedURL is a short-lived, credential-free upload or download URL.
type SignedURL struct {
	URL       string    `json:"url"`
	ExpiresAt time.Time `json:"expiresAt"`
}

// FilesClient is the file storage client (buckets, files, signed URLs).
type FilesClient struct{ c *Client }

// Files returns the file storage client.
func (c *Client) Files() *FilesClient { return &FilesClient{c: c} }

// FileBucket scopes file operations to one bucket.
type FileBucket struct {
	name string
	c    *Client
}

// Bucket returns a handle for file operations in the named bucket.
func (f *FilesClient) Bucket(name string) *FileBucket { return &FileBucket{name: name, c: f.c} }

// escapeFilePath escapes each segment and keeps "/" so the server's
// {path...} wildcard sees the real hierarchy. An empty, "." or ".." segment is
// refused: the HTTP client resolves dot segments in the URL, so
// "../../b/objects/x" would reach another bucket.
func escapeFilePath(p string) (string, error) {
	segs := strings.Split(p, "/")
	for i, s := range segs {
		if badSegment(s) {
			return "", NewError(fmt.Sprintf("invalid file path %q: empty, \".\" and \"..\" segments are not allowed", p), "VALIDATION", false)
		}
		segs[i] = url.PathEscape(s)
	}
	return strings.Join(segs, "/"), nil
}

// codeSignedURLsDisabled is the server error code for a bucket with signed URLs off.
const codeSignedURLsDisabled = "SIGNED_URLS_DISABLED"

func badSegment(s string) bool { return s == "" || s == "." || s == ".." }

// escapeBucketName escapes a bucket name as one segment ("/" included). An
// empty, "." or ".." name is refused for the reason escapeFilePath gives.
func escapeBucketName(name string) (string, error) {
	if badSegment(name) {
		return "", NewError(fmt.Sprintf("invalid bucket name %q: empty, \".\" and \"..\" are not allowed", name), "VALIDATION", false)
	}
	return url.PathEscape(name), nil
}

// restJSON returns nil, not a zero value, when the call fails. A POST is sent
// once: move, copy and create are not idempotent, so a retry after a lost
// response could fail or repeat the change.
func restJSON[T any](ctx context.Context, c *Client, method, path string, body any) (*T, error) {
	var out T
	var err error
	if method == http.MethodPost {
		var b []byte
		if b, err = json.Marshal(body); err != nil {
			return nil, WrapError(err, "failed to marshal request body", "MARSHAL_ERROR", false)
		}
		err = c.executeRequest(ctx, c.httpClient, method, c.serverURL+path, b, &out)
	} else {
		err = c.RestRequest(ctx, method, path, body, &out)
	}
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// CreateBucket creates a bucket; unset cfg fields take the server defaults.
func (f *FilesClient) CreateBucket(ctx context.Context, name string, cfg FileBucketConfig) (*FileBucketInfo, error) {
	body := struct {
		Name string `json:"name"`
		FileBucketConfig
	}{name, cfg}
	return restJSON[FileBucketInfo](ctx, f.c, http.MethodPost, "/api/v1/files/buckets", body)
}

// ListBuckets returns every file bucket in the environment.
func (f *FilesClient) ListBuckets(ctx context.Context) ([]FileBucketInfo, error) {
	var out struct {
		Buckets []FileBucketInfo `json:"buckets"`
	}
	if err := f.c.RestRequest(ctx, http.MethodGet, "/api/v1/files/buckets", nil, &out); err != nil {
		return nil, err
	}
	return out.Buckets, nil
}

// GetBucket returns one bucket's configuration.
func (f *FilesClient) GetBucket(ctx context.Context, name string) (*FileBucketInfo, error) {
	bn, err := escapeBucketName(name)
	if err != nil {
		return nil, err
	}
	return restJSON[FileBucketInfo](ctx, f.c, http.MethodGet, fmt.Sprintf("/api/v1/files/buckets/%s", bn), nil)
}

// UpdateBucket changes the set cfg fields; the change applies to new writes only.
func (f *FilesClient) UpdateBucket(ctx context.Context, name string, cfg FileBucketConfig) (*FileBucketInfo, error) {
	bn, err := escapeBucketName(name)
	if err != nil {
		return nil, err
	}
	return restJSON[FileBucketInfo](ctx, f.c, http.MethodPatch, fmt.Sprintf("/api/v1/files/buckets/%s", bn), cfg)
}

// DeleteBucket deletes an empty bucket; a bucket with files returns ErrConflict.
func (f *FilesClient) DeleteBucket(ctx context.Context, name string) error {
	bn, err := escapeBucketName(name)
	if err != nil {
		return err
	}
	return f.c.RestRequest(ctx, http.MethodDelete, fmt.Sprintf("/api/v1/files/buckets/%s", bn), nil, nil)
}

// Put uploads size bytes from body. An io.ReadSeeker body is rewound and
// retried on a retryable failure; any other reader is sent once, because a
// consumed stream cannot be replayed.
func (b *FileBucket) Put(ctx context.Context, path string, body io.Reader, size int64, opts PutFileOptions) (*FileInfo, error) {
	bn, err := escapeBucketName(b.name)
	if err != nil {
		return nil, err
	}
	if opts.ContentType == "" {
		return nil, NewError("PutFileOptions.ContentType is required", "VALIDATION", false)
	}
	ep, err := escapeFilePath(path)
	if err != nil {
		return nil, err
	}
	hdr := http.Header{}
	hdr.Set("Content-Type", opts.ContentType)
	for k, v := range opts.Metadata {
		hdr.Set("X-Ironflow-Meta-"+k, v)
	}
	if opts.IfMatch != "" {
		hdr.Set("If-Match", opts.IfMatch)
	}
	if opts.IfNoneMatch {
		hdr.Set("If-None-Match", "*")
	}
	seeker, replayable := body.(io.ReadSeeker)
	var start int64
	if replayable {
		if start, err = seeker.Seek(0, io.SeekCurrent); err != nil {
			replayable = false
		}
	}
	var out FileInfo
	attempt := func() error {
		if replayable {
			if _, err := seeker.Seek(start, io.SeekStart); err != nil {
				return WrapError(err, "rewind upload body", "REQUEST_ERROR", false)
			}
		}
		out = FileInfo{}
		resp, err := b.c.rawRequest(ctx, http.MethodPut, fmt.Sprintf("/api/v1/files/buckets/%s/objects/%s", bn, ep), io.NopCloser(body), size, hdr)
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			return WrapError(err, "decode file info", "DECODE_ERROR", false)
		}
		return nil
	}
	run := attempt
	if replayable {
		run = func() error { return b.c.withRetry(ctx, attempt) }
	}
	if err := run(); err != nil {
		if ife, ok := err.(*IronflowError); ok && ife.Retryable && !replayable {
			return nil, fmt.Errorf("upload body is a stream and cannot be replayed, so the request was not retried: %w", err)
		}
		return nil, err
	}
	return &out, nil
}

// rawRequest sends a non-JSON body (or none) and returns the response for
// the caller to read. Non-2xx responses become *IronflowError.
func (c *Client) rawRequest(ctx context.Context, method, path string, body io.ReadCloser, size int64, hdr http.Header) (*http.Response, error) {
	var rb io.Reader
	if body != nil {
		rb = body
		// A non-nil body with ContentLength 0 is sent chunked, which the
		// server rejects with 411; NoBody sends Content-Length: 0.
		if size == 0 {
			rb = http.NoBody
		}
	}
	req, err := http.NewRequestWithContext(ctx, method, c.serverURL+path, rb)
	if err != nil {
		return nil, WrapError(err, "failed to create request", "REQUEST_ERROR", false)
	}
	if body != nil && size > 0 {
		req.ContentLength = size
	}
	maps.Copy(req.Header, hdr)
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	if c.environment != "" {
		req.Header.Set(HeaderEnvironment, c.environment)
	}
	if rid := runIDFromContext(ctx); rid != "" {
		req.Header.Set(HeaderRunID, rid)
	}
	// Client.Timeout (30 s by default) covers the whole exchange, the request
	// body and the response body included. It would cut a large upload, and a
	// download while the caller still reads it. ctx bounds these calls instead.
	hc := *c.httpClient
	hc.Timeout = 0
	resp, err := hc.Do(req)
	if err != nil {
		return nil, WrapError(err, "request failed", "REQUEST_FAILED", true)
	}
	if resp.StatusCode >= 400 {
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return nil, c.errorFromResponse(resp, b)
	}
	return resp, nil
}

// Get returns the file body as a stream; the caller closes it.
func (b *FileBucket) Get(ctx context.Context, path string, opts GetFileOptions) (*FileObject, error) {
	bn, err := escapeBucketName(b.name)
	if err != nil {
		return nil, err
	}
	hdr := http.Header{}
	if opts.IfMatch != "" {
		hdr.Set("If-Match", opts.IfMatch)
	}
	if opts.Range != "" {
		hdr.Set("Range", opts.Range)
	}
	ep, err := escapeFilePath(path)
	if err != nil {
		return nil, err
	}
	var resp *http.Response
	err = b.c.withRetry(ctx, func() error {
		var err error
		//nolint:bodyclose // the body is handed to the caller in FileObject.Body
		resp, err = b.c.rawRequest(ctx, http.MethodGet, fmt.Sprintf("/api/v1/files/buckets/%s/objects/%s", bn, ep), nil, 0, hdr)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &FileObject{Body: resp.Body, ContentType: resp.Header.Get("Content-Type"),
		ETag: strings.Trim(resp.Header.Get("ETag"), `"`), Size: resp.ContentLength}, nil
}

// Info returns a file's metadata without its bytes.
func (b *FileBucket) Info(ctx context.Context, path string) (*FileInfo, error) {
	bn, err := escapeBucketName(b.name)
	if err != nil {
		return nil, err
	}
	ep, err := escapeFilePath(path)
	if err != nil {
		return nil, err
	}
	return restJSON[FileInfo](ctx, b.c, http.MethodGet, fmt.Sprintf("/api/v1/files/buckets/%s/info/%s", bn, ep), nil)
}

// List returns one page of files, ordered by path.
func (b *FileBucket) List(ctx context.Context, opts ListFilesOptions) (*ListFilesResult, error) {
	bn, err := escapeBucketName(b.name)
	if err != nil {
		return nil, err
	}
	q := url.Values{}
	if opts.Prefix != "" {
		q.Set("prefix", opts.Prefix)
	}
	if opts.Delimiter != "" {
		q.Set("delimiter", opts.Delimiter)
	}
	if opts.Cursor != "" {
		q.Set("cursor", opts.Cursor)
	}
	if opts.Limit > 0 {
		q.Set("limit", strconv.Itoa(opts.Limit))
	}
	p := fmt.Sprintf("/api/v1/files/buckets/%s/objects", bn)
	if len(q) > 0 {
		p += "?" + q.Encode()
	}
	return restJSON[ListFilesResult](ctx, b.c, http.MethodGet, p, nil)
}

// Delete removes a file; a missing path is not an error.
func (b *FileBucket) Delete(ctx context.Context, path string) error {
	bn, err := escapeBucketName(b.name)
	if err != nil {
		return err
	}
	ep, err := escapeFilePath(path)
	if err != nil {
		return err
	}
	return b.c.RestRequest(ctx, http.MethodDelete, fmt.Sprintf("/api/v1/files/buckets/%s/objects/%s", bn, ep), nil, nil)
}

// Move renames one file, optionally into another bucket. No bytes move.
func (b *FileBucket) Move(ctx context.Context, from, to string, opts MoveFileOptions) (*FileInfo, error) {
	bn, err := escapeBucketName(b.name)
	if err != nil {
		return nil, err
	}
	body := map[string]string{"from": from, "to": to}
	if opts.ToBucket != "" {
		body["toBucket"] = opts.ToBucket
	}
	if opts.IfMatch != "" {
		body["ifMatch"] = opts.IfMatch
	}
	return restJSON[FileInfo](ctx, b.c, http.MethodPost, fmt.Sprintf("/api/v1/files/buckets/%s/move", bn), body)
}

// Copy duplicates one file, optionally into another bucket.
func (b *FileBucket) Copy(ctx context.Context, from, to string, opts CopyFileOptions) (*FileInfo, error) {
	bn, err := escapeBucketName(b.name)
	if err != nil {
		return nil, err
	}
	body := map[string]string{"from": from, "to": to}
	if opts.ToBucket != "" {
		body["toBucket"] = opts.ToBucket
	}
	return restJSON[FileInfo](ctx, b.c, http.MethodPost, fmt.Sprintf("/api/v1/files/buckets/%s/copy", bn), body)
}

// MovePrefix renames every file under from/ to to/ in one transaction and
// returns how many moved. Both prefixes end in "/".
func (b *FileBucket) MovePrefix(ctx context.Context, from, to string) (int, error) {
	bn, err := escapeBucketName(b.name)
	if err != nil {
		return 0, err
	}
	body := struct {
		From string `json:"from"`
		To   string `json:"to"`
	}{from, to}
	out, err := restJSON[struct {
		Count int `json:"count"`
	}](ctx, b.c, http.MethodPost, fmt.Sprintf("/api/v1/files/buckets/%s/move-prefix", bn), body)
	if err != nil {
		return 0, err
	}
	return out.Count, nil
}

// SignUpload returns a URL a credential-free client can PUT one file to.
func (b *FileBucket) SignUpload(ctx context.Context, path string, opts SignUploadOptions) (*SignedURL, error) {
	bn, err := escapeBucketName(b.name)
	if err != nil {
		return nil, err
	}
	body := map[string]any{"path": path}
	if opts.TTL > 0 {
		body["ttlSeconds"] = int64(opts.TTL / time.Second)
	}
	if opts.MaxBytes > 0 {
		body["maxBytes"] = opts.MaxBytes
	}
	if opts.ContentType != "" {
		body["contentType"] = opts.ContentType
	}
	if opts.CreateOnly {
		body["createOnly"] = true
	}
	// Retried, unlike the other files POSTs: a sign call only mints a URL.
	var out SignedURL
	if err := b.c.RestRequest(ctx, http.MethodPost, fmt.Sprintf("/api/v1/files/buckets/%s/signed-urls/upload", bn), body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// SignDownload returns a URL a credential-free client can GET a file from.
func (b *FileBucket) SignDownload(ctx context.Context, path string, ttl time.Duration) (*SignedURL, error) {
	bn, err := escapeBucketName(b.name)
	if err != nil {
		return nil, err
	}
	body := map[string]any{"path": path}
	if ttl > 0 {
		body["ttlSeconds"] = int64(ttl / time.Second)
	}
	// Retried, unlike the other files POSTs: a sign call only mints a URL.
	var out SignedURL
	if err := b.c.RestRequest(ctx, http.MethodPost, fmt.Sprintf("/api/v1/files/buckets/%s/signed-urls/download", bn), body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
