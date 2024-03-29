// Package bfss3 abstracts Amazon S3 bucket.
//
// When imported, it registers a global `s3://` scheme resolver and can be used like:
//
//	import (
//	  "github.com/bsm/bfs"
//
//	  _ "github.com/bsm/bfs/bfss3"
//	)
//
//	func main() {
//	  ctx := context.Background()
//	  b, _ := bfs.Connect(ctx, "s3://bucket/a&acl=MY_ACL")
//	  f, _ := b.Open(ctx, "b/c.txt") // opens s3://bucket/a/b/c.txt
//	  ...
//	}
//
// bfs.Connect supports the following query parameters:
//
//	aws_access_key_id      - custom AWS credentials
//	aws_secret_access_key  - custom AWS credentials
//	aws_session_token      - custom AWS credentials
//	assume_role            - specify an AWS role ARN to assume
//	region                 - specify an AWS region
//	max_retries            - specify maximum number of retries
//	acl                    - custom ACL, defaults to DefaultACL
//	sse                    - server-side-encryption algorithm
package bfss3

import (
	"context"
	"errors"
	"io"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bmatcuk/doublestar/v3"
	"github.com/bsm/bfs"
	"github.com/bsm/bfs/internal"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
)

// DefaultACL is the default ACL setting.
const DefaultACL = "bucket-owner-full-control"

func init() {
	bfs.Register("s3", func(ctx context.Context, u *url.URL) (bfs.Bucket, error) {
		query := u.Query()

		prefix := u.Path
		if prefix == "" {
			prefix = query.Get("prefix")
		}

		var opts []func(*config.LoadOptions) error

		if s := query.Get("aws_access_key_id"); s != "" {
			creds := aws.NewCredentialsCache(credentials.NewStaticCredentialsProvider(
				s,
				query.Get("aws_secret_access_key"),
				query.Get("aws_session_token"),
			))
			opts = append(opts, config.WithCredentialsProvider(creds))
		}
		if s := query.Get("region"); s != "" {
			opts = append(opts, config.WithRegion(s))
		}
		if s := query.Get("max_retries"); s != "" {
			maxRetries, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return nil, err
			}
			opts = append(opts, config.WithRetryer(func() aws.Retryer {
				return retry.NewStandard(func(o *retry.StandardOptions) {
					o.MaxAttempts = int(maxRetries)
				})
			}))
		}

		cfg, err := config.LoadDefaultConfig(ctx, opts...)
		if err != nil {
			return nil, err
		}

		if s := query.Get("assume_role"); s != "" {
			cfg.Credentials = aws.NewCredentialsCache(
				stscreds.NewAssumeRoleProvider(sts.NewFromConfig(cfg), s),
			)
		}

		return New(ctx, u.Host, &Config{
			Prefix:           prefix,
			ACL:              query.Get("acl"),
			SSE:              query.Get("sse"),
			GrantFullControl: query.Get("grant-full-control"),
			AWS:              &cfg,
		})
	})
}

// Config is passed to New to configure the S3 connection.
type Config struct {
	// Native AWS configuration.
	AWS *aws.Config
	// Custom ACL, defaults to DefaultACL.
	ACL string
	// GrantFullControl setting.
	GrantFullControl string
	// The Server-side encryption algorithm used when storing this object in S3.
	SSE string
	// An optional path prefix
	Prefix string
}

func (c *Config) norm(ctx context.Context) error {
	if c.AWS == nil {
		aws, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			return err
		}
		c.AWS = &aws
	}

	if c.ACL == "" && c.GrantFullControl == "" {
		c.ACL = DefaultACL
	}

	c.Prefix = strings.TrimLeft(c.Prefix, "/")
	if c.Prefix != "" && !strings.HasSuffix(c.Prefix, "/") {
		c.Prefix = c.Prefix + "/"
	}

	return nil
}

type bucket struct {
	*s3.Client
	bucket   string
	config   *Config
	uploader *manager.Uploader
}

// New initiates an bfs.Bucket backed by S3.
func New(ctx context.Context, name string, c *Config) (bfs.Bucket, error) {
	cfg := new(Config)
	if c != nil {
		*cfg = *c
	}
	if err := cfg.norm(ctx); err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(*cfg.AWS)
	uploader := manager.NewUploader(client)

	return &bucket{
		Client:   client,
		bucket:   name,
		config:   cfg,
		uploader: uploader,
	}, nil
}

func (b *bucket) stripPrefix(name string) string {
	if b.config.Prefix != "" {
		name = strings.TrimPrefix(name, b.config.Prefix)
	}
	return strings.TrimLeft(name, "/")
}

func (b *bucket) withPrefix(name string) string {
	if b.config.Prefix != "" {
		name = internal.WithinNamespace(b.config.Prefix, name)
	}
	return strings.TrimLeft(name, "/")
}

// Glob implements bfs.Bucket.
func (b *bucket) Glob(ctx context.Context, pattern string) (bfs.Iterator, error) {
	// quick sanity check
	if _, err := doublestar.Match(pattern, ""); err != nil {
		return nil, err
	}

	p := s3.NewListObjectsV2Paginator(b, &s3.ListObjectsV2Input{
		Bucket: aws.String(b.bucket),
		Prefix: aws.String(b.config.Prefix),
	})
	return &iterator{
		ctx:       ctx,
		bucket:    b,
		paginator: p,
		pattern:   pattern,
	}, nil
}

// Head implements bfs.Bucket.
func (b *bucket) Head(ctx context.Context, name string) (*bfs.MetaInfo, error) {
	resp, err := b.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(b.withPrefix(name)),
	})
	if err != nil {
		return nil, normError(err)
	}

	return &bfs.MetaInfo{
		Name:        name,
		Size:        aws.ToInt64(resp.ContentLength),
		ModTime:     aws.ToTime(resp.LastModified),
		ContentType: aws.ToString(resp.ContentType),
		Metadata:    bfs.NormMetadata(resp.Metadata),
	}, nil
}

// Open implements bfs.Bucket.
func (b *bucket) Open(ctx context.Context, name string) (bfs.Reader, error) {
	resp, err := b.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(b.withPrefix(name)),
	})
	if err != nil {
		return nil, normError(err)
	}
	return &response{
		ReadCloser:    resp.Body,
		ContentLength: aws.ToInt64(resp.ContentLength),
	}, nil
}

// Create implements bfs.Bucket.
func (b *bucket) Create(ctx context.Context, name string, opts *bfs.WriteOptions) (bfs.Writer, error) {
	f, err := os.CreateTemp("", "bfs-s3")
	if err != nil {
		return nil, err
	}

	return &writer{
		File:   f,
		ctx:    ctx,
		bucket: b,
		name:   name,
		opts:   opts,
	}, nil
}

// Remove implements bfs.Bucket.
func (b *bucket) Remove(ctx context.Context, name string) error {
	_, err := b.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(b.withPrefix(name)),
	})
	return normError(err)
}

// Copy supports copying of objects within the bucket.
func (b *bucket) Copy(ctx context.Context, src, dst string) error {
	source := path.Join("/", b.bucket, b.withPrefix(src))
	_, err := b.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:               aws.String(b.bucket),
		CopySource:           aws.String(source),
		Key:                  aws.String(b.withPrefix(dst)),
		ACL:                  types.ObjectCannedACL(b.config.ACL),
		GrantFullControl:     strPresence(b.config.GrantFullControl),
		ServerSideEncryption: types.ServerSideEncryption(b.config.SSE),
	})
	return err
}

// Close implements bfs.Bucket.
func (*bucket) Close() error { return nil }

// --------------------------------------------------------

type writer struct {
	*os.File

	ctx    context.Context
	bucket *bucket
	name   string
	opts   *bfs.WriteOptions

	closeOnce sync.Once
}

func (w *writer) Discard() error {
	err := context.Canceled
	w.closeOnce.Do(func() {
		// Delete tempfile in the end
		fname := w.Name()
		defer os.Remove(fname)

		// Close tempfile
		err = w.File.Close()
	})

	return err
}

func (w *writer) Commit() error {
	err := context.Canceled
	w.closeOnce.Do(func() {
		// Delete tempfile in the end
		fname := w.Name()
		defer os.Remove(fname)

		// Close tempfile
		if err = w.File.Close(); err != nil {
			return
		}

		// Re-open tempfile for reading
		var file *os.File
		if file, err = os.Open(fname); err != nil {
			return
		}
		defer file.Close()

		// Upload file
		_, err = w.bucket.uploader.Upload(w.ctx, &s3.PutObjectInput{
			Bucket:               aws.String(w.bucket.bucket),
			Key:                  aws.String(w.bucket.withPrefix(w.name)),
			Body:                 file,
			ContentType:          aws.String(w.opts.GetContentType()),
			Metadata:             w.opts.GetMetadata(),
			ACL:                  types.ObjectCannedACL(w.bucket.config.ACL),
			GrantFullControl:     strPresence(w.bucket.config.GrantFullControl),
			ServerSideEncryption: types.ServerSideEncryption(w.bucket.config.SSE),
		})
	})

	return normError(err)
}

// -----------------------------------------------------------------------------

func normError(err error) error {
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "NotFound", "NoSuchKey", "NoSuchBucket":
				return bfs.ErrNotFound
			}
		}
	}
	return err
}

func strPresence(s string) *string {
	if s != "" {
		return aws.String(s)
	}
	return nil
}

type response struct {
	io.ReadCloser
	ContentLength int64
}

func (r *response) Read(p []byte) (n int, err error) {
	if r.ContentLength <= 0 {
		return 0, io.EOF
	}

	if int64(len(p)) > r.ContentLength {
		p = p[:r.ContentLength]
	}
	n, err = r.ReadCloser.Read(p)
	if err == io.EOF && n > 0 && int64(n) == r.ContentLength {
		err = nil
	}
	r.ContentLength -= int64(n)
	return
}

// --------------------------------------------------------------------

type iterator struct {
	ctx context.Context

	bucket    *bucket
	paginator *s3.ListObjectsV2Paginator
	pattern   string

	err  error
	last bool // indicates last page
	pos  int
	page []object
}

type object struct {
	key     string
	size    int64
	modTime time.Time
}

func (i *iterator) Close() error {
	i.last = true
	i.pos = len(i.page)
	return nil
}

func (i *iterator) Name() string {
	if i.pos < len(i.page) {
		return i.page[i.pos].key
	}
	return ""
}

func (i *iterator) Size() int64 {
	if i.pos < len(i.page) {
		return i.page[i.pos].size
	}
	return 0
}

func (i *iterator) ModTime() time.Time {
	if i.pos < len(i.page) {
		return i.page[i.pos].modTime
	}
	return time.Time{}
}

func (i *iterator) Next() bool {
	if i.err != nil {
		return false
	}

	if i.pos++; i.pos < len(i.page) {
		return true
	}

	if !i.paginator.HasMorePages() {
		return false
	}

	if err := i.fetchNextPage(); err != nil {
		i.err = err
		return false
	}
	return i.Next()
}

func (i *iterator) Error() error { return i.err }

func (i *iterator) fetchNextPage() error {
	i.page = i.page[:0]
	i.pos = -1

	res, err := i.paginator.NextPage(i.ctx)
	if err != nil {
		return err
	}

	for _, obj := range res.Contents {
		name := i.bucket.stripPrefix(aws.ToString(obj.Key))
		if ok, err := doublestar.Match(i.pattern, name); err != nil {
			return err
		} else if ok {
			i.page = append(i.page, object{
				key:     name,
				size:    aws.ToInt64(obj.Size),
				modTime: aws.ToTime(obj.LastModified),
			})
		}
	}
	return nil
}
