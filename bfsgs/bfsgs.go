package bfsgs

import (
	"context"
	"io"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/bsm/bfs"
	"google.golang.org/api/iterator"
)

type gsBucket struct {
	bucket *storage.BucketHandle
	config *Config
}

// Config is passed to New to configure the S3 connection.
type Config struct {
	Prefix string // an optional path prefix
}

func (c *Config) norm() {
}

// New initiates an bfs.Bucket backed by Google Cloud Storage.
func New(ctx context.Context, bucket string, cfg *Config) (bfs.Bucket, error) {
	config := new(Config)
	if cfg != nil {
		*config = *cfg
	}
	config.norm()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, err
	}

	return &gsBucket{
		bucket: client.Bucket(bucket),
		config: config,
	}, nil
}

func (b *gsBucket) stripPrefix(name string) string {
	if b.config.Prefix == "" {
		return name
	}
	name = strings.TrimPrefix(name, b.config.Prefix)
	name = strings.TrimPrefix(name, "/")
	return name
}

func (b *gsBucket) withPrefix(name string) string {
	if b.config.Prefix == "" {
		return name
	}
	return path.Join(b.config.Prefix, name)
}

// Glob implements bfs.Bucket.
func (b *gsBucket) Glob(ctx context.Context, pattern string) ([]string, error) {
	var matches []string

	iter := b.bucket.Objects(ctx, &storage.Query{
		Prefix: b.config.Prefix,
	})
	for {
		obj, err := iter.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return nil, err
		}

		name := b.stripPrefix(obj.Name)
		if ok, err := path.Match(pattern, name); err != nil {
			return nil, err
		} else if ok {
			matches = append(matches, name)
		}
	}
	return matches, nil
}

// Head implements bfs.Bucket.
func (b *gsBucket) Head(ctx context.Context, name string) (*bfs.MetaInfo, error) {
	obj := b.bucket.Object(b.withPrefix(name))
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return nil, normError(err)
	}

	return &bfs.MetaInfo{
		Name:    name,
		Size:    attrs.Size,
		ModTime: attrs.Updated,
	}, nil
}

// Open implements bfs.Bucket.
func (b *gsBucket) Open(ctx context.Context, name string) (io.ReadCloser, error) {
	obj := b.bucket.Object(b.withPrefix(name))
	ord, err := obj.NewReader(ctx)
	return ord, normError(err)
}

// Create implements bfs.Bucket.
func (b *gsBucket) Create(ctx context.Context, name string) (io.WriteCloser, error) {
	obj := b.bucket.Object(b.withPrefix(name))
	return obj.NewWriter(ctx), nil
}

// Remove implements bfs.Bucket.
func (b *gsBucket) Remove(ctx context.Context, name string) error {
	obj := b.bucket.Object(b.withPrefix(name))
	err := obj.Delete(ctx)
	if err == storage.ErrObjectNotExist {
		return nil
	}
	return err
}

// Close implements bfs.Bucket.
func (*gsBucket) Close() error { return nil }

// --------------------------------------------------------------------

func normError(err error) error {
	if err == storage.ErrObjectNotExist {
		return bfs.ErrNotFound
	}
	return err
}
