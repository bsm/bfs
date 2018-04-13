package bfss3

import (
	"context"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/bsm/bfs"
)

type s3Bucket struct {
	s3iface.S3API
	bucket string
	config *Config
}

// Config is passed to New to configure the S3 connection.
type Config struct {
	AWS    aws.Config // native AWS configuration
	Prefix string     // an optional path prefix
	ACL    string     // custom ACL, defaults to 'bucket-owner-full-control'
}

func (c *Config) norm() {
	if c.ACL == "" {
		c.ACL = "bucket-owner-full-control"
	}
}

// New initiates an bfs.Bucket backed by S3.
func New(bucket string, cfg *Config) (bfs.Bucket, error) {
	config := new(Config)
	if cfg != nil {
		*config = *cfg
	}

	sess, err := session.NewSession(&config.AWS)
	if err != nil {
		return nil, err
	}

	return &s3Bucket{
		bucket: bucket,
		config: config,
		S3API:  s3.New(sess),
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
	return path.Join(b.config.Prefix, name)
}

// Glob implements bfs.Bucket.
func (b *s3Bucket) Glob(ctx context.Context, pattern string) ([]string, error) {
	var matches []string

	eachPage := func(page *s3.ListObjectsV2Output, _ bool) bool {
		for _, obj := range page.Contents {
			name := b.stripPrefix(aws.StringValue(obj.Key))

			if ok, err := path.Match(pattern, name); err != nil {
				return false
			} else if ok {
				matches = append(matches, name)
			}
		}
		return true
	}

	err := b.ListObjectsV2PagesWithContext(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(b.bucket),
		Prefix: aws.String(b.config.Prefix),
	}, eachPage)
	if err != nil {
		return nil, err
	}
	return matches, nil
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
		Name:    name,
		Size:    aws.Int64Value(resp.ContentLength),
		ModTime: aws.TimeValue(resp.LastModified),
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
func (b *s3Bucket) Create(ctx context.Context, name string) (io.WriteCloser, error) {
	f, err := ioutil.TempFile("", "bfs-s3")
	if err != nil {
		return nil, err
	}

	return &writer{
		File:   f,
		ctx:    ctx,
		bucket: b,
		name:   name,
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

// Close implements bfs.Bucket.
func (*s3Bucket) Close() error { return nil }

// --------------------------------------------------------

type writer struct {
	*os.File

	ctx    context.Context
	bucket *s3Bucket
	name   string

	closeOnce sync.Once
}

func (w *writer) Close() (err error) {
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

		_, err = w.bucket.PutObjectWithContext(w.ctx, &s3.PutObjectInput{
			ACL:    aws.String(w.bucket.config.ACL),
			Bucket: aws.String(w.bucket.bucket),
			Key:    aws.String(w.bucket.withPrefix(w.name)),
			Body:   file,
		})
	})
	return
}

// -----------------------------------------------------------------------------

func normError(err error) error {
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
		}
	}
	return err
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
