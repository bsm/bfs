// Package bfss3 abstracts Amazon S3 bucket.
//
// When imported, it registers a global `s3://` scheme resolver and can be used like:
//
//   import (
//     "github.com/bsm/bfs"
//
//     _ "github.com/bsm/bfs/bfss3"
//   )
//
//   func main() {
//     ctx := context.Background()
//     b, _ := bfs.Connect(ctx, "s3://bucket/a&acl=MY_ACL")
//     f, _ := b.Open(ctx, "b/c.txt") // opens s3://bucket/a/b/c.txt
//     ...
//   }
//
// bfs.Connect supports the following query parameters:
//
//   aws_access_key_id      - custom AWS credentials
//   aws_secret_access_key  - custom AWS credentials
//   aws_session_token      - custom AWS credentials
//   region                 - specify an AWS region
//   max_retries            - specify maximum number of retries
//   acl                    - custom ACL, defaults to DefaultACL
//   sse                    - server-side-encryption algorithm
//
package bfss3

import (
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/bmatcuk/doublestar"
	"github.com/bsm/bfs"
	"github.com/bsm/bfs/internal"
)

// DefaultACL is the default ACL setting.
const DefaultACL = "bucket-owner-full-control"

func init() {
	bfs.Register("s3", func(ctx context.Context, u *url.URL) (bfs.Bucket, error) {
		query := u.Query()
		awscfg := aws.Config{}

		if s := query.Get("aws_access_key_id"); s != "" {
			awscfg.Credentials = credentials.NewStaticCredentials(
				s,
				query.Get("aws_secret_access_key"),
				query.Get("aws_session_token"),
			)
		}
		if s := query.Get("region"); s != "" {
			awscfg.Region = aws.String(s)
		}
		if s := query.Get("max_retries"); s != "" {
			if n, err := strconv.Atoi(s); err == nil {
				awscfg.MaxRetries = aws.Int(n)
			}
		}

		prefix := u.Path
		if prefix == "" {
			prefix = query.Get("prefix")
		}

		return New(u.Host, &Config{
			Prefix:           prefix,
			ACL:              query.Get("acl"),
			SSE:              query.Get("sse"),
			GrantFullControl: query.Get("grant-full-control"),
			AWS:              awscfg,
		})
	})
}

// Config is passed to New to configure the S3 connection.
type Config struct {
	// Native AWS configuration, used to create a Session,
	// unless one is already passed.
	AWS aws.Config
	// Custom ACL, defaults to DefaultACL.
	ACL string
	// GrantFullControl setting.
	GrantFullControl string
	// The Server-side encryption algorithm used when storing this object in S3.
	SSE string
	// An optional path prefix
	Prefix string
	// An optional custom session.
	// If nil, a new session will be created using the AWS config.
	Session *session.Session
}

func (c *Config) norm() error {
	if c.ACL == "" {
		c.ACL = DefaultACL
	}

	if c.Session == nil {
		sess, err := session.NewSession(&c.AWS, &aws.Config{
			HTTPClient: newHTTPClientWithoutCompression(),
		})
		if err != nil {
			return err
		} else {
			c.Session = sess
		}
	}

	c.Prefix = strings.TrimPrefix(c.Prefix, "/")
	if c.Prefix != "" && !strings.HasSuffix(c.Prefix, "/") {
		c.Prefix = c.Prefix + "/"
	}

	return nil
}

type s3Bucket struct {
	s3iface.S3API
	bucket   string
	config   *Config
	uploader *s3manager.Uploader
}

// New initiates an bfs.Bucket backed by S3.
func New(bucket string, cfg *Config) (bfs.Bucket, error) {
	config := new(Config)
	if cfg != nil {
		*config = *cfg
	}
	if err := config.norm(); err != nil {
		return nil, err
	}

	client := s3.New(config.Session)

	return &s3Bucket{
		S3API:    client,
		bucket:   bucket,
		config:   config,
		uploader: s3manager.NewUploaderWithClient(client),
	}, nil
}

func (b *s3Bucket) stripPrefix(name string) string {
	if b.config.Prefix == "" {
		return name
	}
	name = strings.TrimPrefix(name, b.config.Prefix)
	name = strings.TrimPrefix(name, "/")
	return name
}

func (b *s3Bucket) withPrefix(name string) string {
	if b.config.Prefix == "" {
		return name
	}
	return internal.WithinNamespace(b.config.Prefix, name)
}

// Glob implements bfs.Bucket.
func (b *s3Bucket) Glob(ctx context.Context, pattern string) (bfs.Iterator, error) {
	// quick sanity check
	if _, err := doublestar.Match(pattern, ""); err != nil {
		return nil, err
	}

	return &iterator{
		parent:  b,
		ctx:     ctx,
		pattern: pattern,
	}, nil
}

// Head implements bfs.Bucket.
func (b *s3Bucket) Head(ctx context.Context, name string) (*bfs.MetaInfo, error) {
	resp, err := b.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(b.withPrefix(name)),
	})
	if err != nil {
		return nil, normError(err)
	}

	return &bfs.MetaInfo{
		Name:        name,
		Size:        aws.Int64Value(resp.ContentLength),
		ModTime:     aws.TimeValue(resp.LastModified),
		ContentType: aws.StringValue(resp.ContentType),
		Metadata:    aws.StringValueMap(resp.Metadata),
	}, nil
}

// Open implements bfs.Bucket.
func (b *s3Bucket) Open(ctx context.Context, name string) (io.ReadCloser, error) {
	resp, err := b.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(b.withPrefix(name)),
	})
	if err != nil {
		return nil, normError(err)
	}
	return &response{
		ReadCloser:    resp.Body,
		ContentLength: aws.Int64Value(resp.ContentLength),
	}, nil
}

// Create implements bfs.Bucket.
func (b *s3Bucket) Create(ctx context.Context, name string, opts *bfs.WriteOptions) (io.WriteCloser, error) {
	f, err := ioutil.TempFile("", "bfs-s3")
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
func (b *s3Bucket) Remove(ctx context.Context, name string) error {
	_, err := b.DeleteObjectWithContext(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(b.bucket),
		Key:    aws.String(b.withPrefix(name)),
	})
	return normError(err)
}

// Copy supports copying of objects within the bucket.
func (b *s3Bucket) Copy(ctx context.Context, src, dst string) error {
	source := path.Join("/", b.bucket, b.withPrefix(src))
	_, err := b.CopyObjectWithContext(ctx, &s3.CopyObjectInput{
		Bucket:               aws.String(b.bucket),
		CopySource:           aws.String(source),
		Key:                  aws.String(b.withPrefix(dst)),
		ACL:                  strPresence(b.config.ACL),
		GrantFullControl:     strPresence(b.config.GrantFullControl),
		ServerSideEncryption: strPresence(b.config.SSE),
	})
	return err
}

// Close implements bfs.Bucket.
func (*s3Bucket) Close() error { return nil }

// --------------------------------------------------------

type writer struct {
	*os.File

	ctx    context.Context
	bucket *s3Bucket
	name   string
	opts   *bfs.WriteOptions

	closeOnce sync.Once
}

func (w *writer) Close() error {
	var err error

	w.closeOnce.Do(func() {
		// Delete tempfile in the end
		fname := w.Name()
		defer os.Remove(fname)

		// Re-open tempfile for reading
		if err2 := w.File.Close(); err2 != nil {
			err = err2
			return
		}

		file, err2 := os.Open(fname)
		if err2 != nil {
			err = err2
			return
		}
		defer file.Close()

		_, err = w.bucket.uploader.UploadWithContext(w.ctx, &s3manager.UploadInput{
			Bucket:               aws.String(w.bucket.bucket),
			Key:                  aws.String(w.bucket.withPrefix(w.name)),
			Body:                 file,
			ContentType:          aws.String(w.opts.GetContentType()),
			Metadata:             aws.StringMap(w.opts.GetMetadata()),
			ACL:                  strPresence(w.bucket.config.ACL),
			GrantFullControl:     strPresence(w.bucket.config.GrantFullControl),
			ServerSideEncryption: strPresence(w.bucket.config.SSE),
		})
	})

	return normError(err)
}

// -----------------------------------------------------------------------------

func normError(err error) error {
	if err == nil {
		return nil
	}

	switch e := err.(type) {
	case awserr.RequestFailure:
		switch e.StatusCode() {
		case http.StatusNotFound:
			return bfs.ErrNotFound
		}
	case awserr.Error:
		switch e.Code() {
		case s3.ErrCodeNoSuchKey:
			return bfs.ErrNotFound
		case request.CanceledErrorCode:
			return context.Canceled
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
	parent  *s3Bucket
	ctx     context.Context
	pattern string
	token   *string

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

	if i.last {
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

	res, err := i.parent.ListObjectsV2WithContext(i.ctx, &s3.ListObjectsV2Input{
		Bucket:            aws.String(i.parent.bucket),
		Prefix:            aws.String(i.parent.config.Prefix),
		ContinuationToken: i.token,
	})
	if err != nil {
		return err
	}

	i.token = res.NextContinuationToken
	i.last = i.token == nil

	for _, obj := range res.Contents {
		if obj == nil {
			continue
		}

		name := i.parent.stripPrefix(aws.StringValue(obj.Key))
		if ok, err := doublestar.Match(i.pattern, name); err != nil {
			return err
		} else if ok {
			i.page = append(i.page, object{
				key:     name,
				size:    aws.Int64Value(obj.Size),
				modTime: aws.TimeValue(obj.LastModified),
			})
		}
	}
	return nil
}

// --------------------------------------------------------------------

// newHTTPClientWithoutCompression returns an HTTP client with implicit GZIP compression disabled.
func newHTTPClientWithoutCompression() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.DisableCompression = true
	return &http.Client{Transport: t}
}
