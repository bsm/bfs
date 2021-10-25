// Package bfsgs abstracts Google Cloud Storage bucket.
//
// When imported, it registers a global `gs://` scheme resolver and can be used like:
//
//   import (
//     "github.com/bsm/bfs"
//
//     _ "github.com/bsm/bfs/bfsgs"
//   )
//
//   func main() {
//     ctx := context.Background()
//     b, _ := bfs.Connect(ctx, "gs://bucket/a")
//     f, _ := b.Open(ctx, "b/c.txt") // opens gs://bucket/a/b/c.txt
//     ...
//   }
//
// bfs.Connect supports the following query parameters:
//
//   scopes      - custom scopes
//   credentials - path to custom credentials file
//
package bfsgs

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/bmatcuk/doublestar/v3"
	"github.com/bsm/bfs"
	"github.com/bsm/bfs/internal"
	giterator "google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func init() {
	bfs.Register("gs", func(ctx context.Context, u *url.URL) (bfs.Bucket, error) {
		query := u.Query()

		prefix := u.Path
		if prefix == "" {
			prefix = query.Get("prefix")
		}

		conf := &Config{Prefix: prefix}
		if s := query.Get("scopes"); s != "" {
			conf.Options = append(conf.Options, option.WithScopes(strings.Split(s, ",")...))
		}
		if s := query.Get("credentials"); s != "" {
			conf.Options = append(conf.Options, option.WithCredentialsFile(s))
		}
		if s := query.Get("acl"); s != "" {
			conf.PredefinedACL = s
		}

		return New(ctx, u.Host, conf)
	})
}

// Config is passed to New to configure the Google Cloud Storage connection.
type Config struct {
	Options       []option.ClientOption // options for Google API client
	Prefix        string                // an optional path prefix
	PredefinedACL string                // an optional predefined ACL string, e.g. "publicRead"
}

func (c *Config) norm() error {
	c.Prefix = strings.TrimLeft(c.Prefix, "/")
	if c.Prefix != "" && !strings.HasSuffix(c.Prefix, "/") {
		c.Prefix = c.Prefix + "/"
	}

	return nil
}

type bucket struct {
	bucket *storage.BucketHandle
	config *Config
}

// New initiates an bfs.Bucket backed by Google Cloud Storage.
func New(ctx context.Context, name string, cfg *Config) (bfs.Bucket, error) {
	config := new(Config)
	if cfg != nil {
		*config = *cfg
	}
	if err := config.norm(); err != nil {
		return nil, err
	}

	client, err := storage.NewClient(ctx, config.Options...)
	if err != nil {
		return nil, err
	}

	return &bucket{
		bucket: client.Bucket(name),
		config: config,
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

	iter := b.bucket.Objects(ctx, &storage.Query{
		Prefix: b.config.Prefix,
	})
	return &iterator{
		parent:  b,
		iter:    iter,
		pattern: pattern,
	}, nil
}

// Head implements bfs.Bucket.
func (b *bucket) Head(ctx context.Context, name string) (*bfs.MetaInfo, error) {
	obj := b.bucket.Object(b.withPrefix(name))
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		return nil, normError(err)
	}

	return &bfs.MetaInfo{
		Name:        name,
		Size:        attrs.Size,
		ModTime:     attrs.Updated,
		ContentType: attrs.ContentType,
		Metadata:    bfs.NormMetadata(attrs.Metadata),
	}, nil
}

// Open implements bfs.Bucket.
func (b *bucket) Open(ctx context.Context, name string) (bfs.Reader, error) {
	obj := b.bucket.Object(b.withPrefix(name))
	ord, err := obj.NewReader(ctx)
	return ord, normError(err)
}

// Create implements bfs.Bucket.
func (b *bucket) Create(ctx context.Context, name string, opts *bfs.WriteOptions) (bfs.Writer, error) {
	ctx, cancel := context.WithCancel(ctx)

	obj := b.bucket.Object(b.withPrefix(name))
	wrt := obj.NewWriter(ctx)
	wrt.PredefinedACL = b.config.PredefinedACL
	wrt.ContentType = opts.GetContentType()
	wrt.Metadata = opts.GetMetadata()
	return &writer{Writer: wrt, ctx: ctx, cancel: cancel}, nil
}

// Remove implements bfs.Bucket.
func (b *bucket) Remove(ctx context.Context, name string) error {
	obj := b.bucket.Object(b.withPrefix(name))
	err := obj.Delete(ctx)
	if err == storage.ErrObjectNotExist {
		return nil
	}
	return err
}

// Copy supports copying of objects within the bucket.
func (b *bucket) Copy(ctx context.Context, src, dst string) error {
	_, err := b.bucket.Object(b.withPrefix(dst)).CopierFrom(
		b.bucket.Object(b.withPrefix(src)),
	).Run(ctx)
	return err
}

// Close implements bfs.Bucket.
func (*bucket) Close() error { return nil }

// --------------------------------------------------------------------

func normError(err error) error {
	if err == storage.ErrObjectNotExist {
		return bfs.ErrNotFound
	}
	return err
}

// --------------------------------------------------------------------

type writer struct {
	*storage.Writer
	ctx    context.Context
	cancel context.CancelFunc
}

func (w *writer) Discard() error {
	err := w.ctx.Err()

	w.cancel() // cancel BEFORE close
	if ezz := w.Close(); ezz != nil && !errors.Is(ezz, context.Canceled) {
		err = ezz
	}

	return err
}

func (w *writer) Commit() error {
	err := w.ctx.Err()

	if ezz := w.Close(); ezz != nil {
		err = ezz
	}
	w.cancel() // cancel AFTER close

	return err
}

// --------------------------------------------------------------------

type iterator struct {
	parent  *bucket
	iter    *storage.ObjectIterator
	pattern string
	current object
	err     error
}

type object struct {
	name    string
	size    int64
	modTime time.Time
}

func (*iterator) Close() error         { return nil }
func (i *iterator) Name() string       { return i.current.name }
func (i *iterator) Size() int64        { return i.current.size }
func (i *iterator) ModTime() time.Time { return i.current.modTime }

func (i *iterator) Next() bool {
	if i.err != nil {
		return false
	}

	for {
		obj, err := i.iter.Next()
		if err != nil {
			i.err = err
			return false
		}

		name := i.parent.stripPrefix(obj.Name)
		if ok, err := doublestar.Match(i.pattern, name); err != nil {
			i.err = err
			return false
		} else if ok {
			i.current = object{
				name:    name,
				size:    obj.Size,
				modTime: obj.Updated,
			}
			return true
		}
	}
}

func (i *iterator) Error() error {
	if i.err != giterator.Done {
		return i.err
	}
	return nil
}
