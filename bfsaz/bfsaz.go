// Package bfsaz abstracts Azure Blob Storage.
//
// When imported, it registers a global `az://` scheme resolver and can be used like:
//
//   import (
//     "github.com/bsm/bfs"
//
//     _ "github.com/bsm/bfs/bfsaz"
//   )
//
//   func main() {
//     ctx := context.Background()
//     b, _ := bfs.Connect(ctx, "az://account.blob.core.windows.net/container?access_key=" + os.Getenv("AZURE_STORAGE_ACCESS_KEY"))
//     f, _ := b.Open(ctx, "b/c.txt") // opens a blob for reading
//     ...
//   }
//
// bfs.Connect supports the following query parameters:
//
//   access_key - the Azure storage access key
//
package bfsaz

import (
	"context"
	"errors"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/bmatcuk/doublestar"
	"github.com/bsm/bfs"
	"github.com/bsm/bfs/internal"
)

func init() {
	bfs.Register("az", func(ctx context.Context, u *url.URL) (bfs.Bucket, error) {
		query := u.Query()

		var (
			prefix string
			cred   azblob.Credential
		)

		// extract prefix from path
		path := u.Path
		if len(path) > 2 {
			if i := strings.Index(path[1:], "/"); i > 0 {
				path, prefix = path[:i+1], path[i+2:]
			}
		}

		// fallback on prefix query parameter
		if prefix == "" {
			prefix = query.Get("prefix")
		}

		// check if an access_key was provided
		if s := query.Get("access_key"); s != "" {
			accountName := strings.TrimSuffix(u.Host, ".blob.core.windows.net")

			var err error
			cred, err = azblob.NewSharedKeyCredential(accountName, s)
			if err != nil {
				return nil, err
			}
		}

		return New("https://"+u.Host+path, &Config{
			Prefix:     prefix,
			Credential: cred,
		})
	})
}

// Config is passed to New to configure the S3 connection.
type Config struct {
	// An optional path prefix
	Prefix string

	// Custom credentials. By default it will look for
	// AZURE_STORAGE_ACCESS_KEY env variable and fallback on anonymous access
	// if not found.
	Credential azblob.Credential
}

func (c *Config) norm() error {
	c.Prefix = strings.TrimPrefix(c.Prefix, "/")
	if c.Prefix != "" && !strings.HasSuffix(c.Prefix, "/") {
		c.Prefix = c.Prefix + "/"
	}

	return nil
}

type bucket struct {
	azblob.ContainerURL
	config *Config
}

// New initiates an bfs.Bucket backed by Azure.
func New(containerURL string, cfg *Config) (bfs.Bucket, error) {
	config := new(Config)
	if cfg != nil {
		*config = *cfg
	}
	if err := config.norm(); err != nil {
		return nil, err
	}

	// parse URL
	u, err := url.Parse(containerURL)
	if err != nil {
		return nil, err
	}

	// use configured credential if provided
	credential := config.Credential

	// try to load access key from the environment
	if credential == nil {
		if s := os.Getenv("AZURE_STORAGE_ACCESS_KEY"); s != "" {
			account := strings.TrimSuffix(u.Host, ".blob.core.windows.net")
			credential, err = azblob.NewSharedKeyCredential(account, s)
			if err != nil {
				return nil, err
			}
		}
	}

	// fall back on anonymous access
	if credential == nil {
		credential = azblob.NewAnonymousCredential()
	}

	// init a pipeline and return the bucket
	pipe := azblob.NewPipeline(credential, azblob.PipelineOptions{})
	return &bucket{
		ContainerURL: azblob.NewContainerURL(*u, pipe),
		config:       config,
	}, nil
}

func (b *bucket) stripPrefix(name string) string {
	if b.config.Prefix == "" {
		return name
	}
	name = strings.TrimPrefix(name, b.config.Prefix)
	name = strings.TrimPrefix(name, "/")
	return name
}

func (b *bucket) withPrefix(name string) string {
	if b.config.Prefix == "" {
		return name
	}
	return internal.WithinNamespace(b.config.Prefix, name)
}

// Glob implements bfs.Bucket.
func (b *bucket) Glob(ctx context.Context, pattern string) (bfs.Iterator, error) {
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
func (b *bucket) Head(ctx context.Context, name string) (*bfs.MetaInfo, error) {
	resp, err := b.NewBlockBlobURL(b.withPrefix(name)).
		GetProperties(ctx, azblob.BlobAccessConditions{})
	if err != nil {
		return nil, normError(err)
	}

	return &bfs.MetaInfo{
		Name:        name,
		Size:        resp.ContentLength(),
		ModTime:     resp.LastModified(),
		ContentType: resp.ContentType(),
		Metadata:    bfs.NormMetadata(transKeys(resp.NewMetadata(), "_", "-")),
	}, nil
}

// Open implements bfs.Bucket.
func (b *bucket) Open(ctx context.Context, name string) (bfs.Reader, error) {
	resp, err := b.NewBlockBlobURL(b.withPrefix(name)).
		Download(ctx, 0, azblob.CountToEnd, azblob.BlobAccessConditions{}, false)
	if err != nil {
		return nil, normError(err)
	}

	return &reader{
		ReadCloser:    resp.Body(azblob.RetryReaderOptions{}),
		ContentLength: resp.ContentLength(),
	}, nil
}

// Create implements bfs.Bucket.
func (b *bucket) Create(ctx context.Context, name string, opts *bfs.WriteOptions) (bfs.Writer, error) {
	f, err := ioutil.TempFile("", "bfs-az")
	if err != nil {
		return nil, err
	}

	return &writer{
		File: f,
		ctx:  ctx,
		blob: b.NewBlockBlobURL(b.withPrefix(name)),
		opts: opts,
	}, nil
}

// Remove implements bfs.Bucket.
func (b *bucket) Remove(ctx context.Context, name string) error {
	_, err := b.NewBlockBlobURL(b.withPrefix(name)).
		Delete(ctx, "", azblob.BlobAccessConditions{})
	if ne := normError(err); ne != nil && ne != bfs.ErrNotFound {
		return ne
	}
	return nil
}

// Close implements bfs.Bucket.
func (*bucket) Close() error { return nil }

// --------------------------------------------------------------------

func normError(err error) error {
	if err == nil {
		return nil
	}

	var se azblob.StorageError
	if errors.As(err, &se) {
		if se.ServiceCode() == azblob.ServiceCodeBlobNotFound {
			return bfs.ErrNotFound
		}
		return err
	}

	if pe := pipeline.Cause(err); pe != nil {
		if ue := errors.Unwrap(pe); ue != nil {
			return ue
		}
		return pe
	}

	return err
}

func transKeys(meta map[string]string, from, to string) map[string]string {
	for k, v := range meta {
		if nk := strings.ReplaceAll(k, from, to); nk != k {
			delete(meta, k)
			meta[nk] = v
		}
	}
	return meta
}

// --------------------------------------------------------------------

type writer struct {
	*os.File

	ctx  context.Context
	blob azblob.BlockBlobURL
	opts *bfs.WriteOptions

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
		_, err = w.blob.Upload(
			w.ctx, file,
			azblob.BlobHTTPHeaders{
				ContentType: w.opts.GetContentType(),
			},
			azblob.Metadata(transKeys(w.opts.GetMetadata(), "-", "_")),
			azblob.BlobAccessConditions{},
		)
	})

	return normError(err)
}

// --------------------------------------------------------------------

type iterator struct {
	parent  *bucket
	ctx     context.Context
	pattern string
	marker  azblob.Marker

	err  error
	last bool // indicates last page
	pos  int
	page []azblob.BlobItem
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
		return i.parent.stripPrefix(i.page[i.pos].Name)
	}
	return ""
}

func (i *iterator) Size() int64 {
	if i.pos < len(i.page) {
		if size := i.page[i.pos].Properties.ContentLength; size != nil {
			return *size
		}
	}
	return 0
}

func (i *iterator) ModTime() time.Time {
	if i.pos < len(i.page) {
		return i.page[i.pos].Properties.LastModified
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

	resp, err := i.parent.ListBlobsFlatSegment(i.ctx, i.marker, azblob.ListBlobsSegmentOptions{
		Prefix: i.parent.config.Prefix,
	})
	if err != nil {
		return err
	}

	i.marker = resp.NextMarker
	i.last = !resp.NextMarker.NotDone()

	for _, obj := range resp.Segment.BlobItems {
		name := i.parent.stripPrefix(obj.Name)
		if ok, err := doublestar.Match(i.pattern, name); err != nil {
			return err
		} else if ok {
			i.page = append(i.page, obj)
		}
	}
	return nil
}

type reader struct {
	io.ReadCloser
	ContentLength int64
}

func (r *reader) Read(p []byte) (n int, err error) {
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
