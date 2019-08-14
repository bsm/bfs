// Package bfsftp abstracts an FTP file system.
//
// When imported, it registers a global `ftp://` scheme resolver and can be used like:
//
//   import (
//     "github.com/bsm/bfs"
//
//     _ "github.com/bsm/bfs/bfsftp"
//   )
//
//   func main() {
//     ctx := context.Background()
//     b, _ := bfs.Connect(ctx, "ftp://user:pass@hostname:21/path/to/root?tmpdir=%2Fcustom%2Ftmp")
//     f, _ := b.Open(ctx, "file/within/root.txt")
//     ...
//   }
//
// bfs.Connect supports the following query parameters:
//
//   tmpdir - custom temp dir
//
package bfsftp

import (
	"context"
	"io"
	"io/ioutil"
	"net"
	"net/textproto"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/bmatcuk/doublestar"
	"github.com/bsm/bfs"
	"github.com/bsm/bfs/internal"
	"github.com/jlaffaye/ftp"
)

func init() {
	bfs.Register("ftp", func(_ context.Context, u *url.URL) (bfs.Bucket, error) {
		query := u.Query()
		address := net.JoinHostPort(u.Host, u.Port())

		username, password := "", ""
		if u.User != nil {
			username = u.User.Username()
			password, _ = u.User.Password()
		}

		return New(address, &Config{
			Username: username,
			Password: password,
			Prefix:   u.Path,
			TempDir:  query.Get("tmpdir"),
		})
	})
}

// Config is passed to New to configure the S3 connection.
type Config struct {
	// Username to use.
	Username string
	// Password to use in combination with Username.
	Password string
	// An optional path prefix.
	Prefix string
	// A custom temp dir.
	TempDir string
}

func (c *Config) norm() error {
	c.Prefix = strings.TrimPrefix(c.Prefix, "/")
	if c.Prefix != "" && !strings.HasSuffix(c.Prefix, "/") {
		c.Prefix = c.Prefix + "/"
	}
	return nil
}

type ftpBucket struct {
	conn   *ftp.ServerConn
	config *Config
}

// New initiates an bfs.Bucket backed by FTP.
func New(address string, cfg *Config) (bfs.Bucket, error) {
	config := new(Config)
	if cfg != nil {
		*config = *cfg
	}
	if err := config.norm(); err != nil {
		return nil, err
	}

	conn, err := ftp.Dial(address)
	if err != nil {
		return nil, err
	}

	if config.Username != "" {
		if err := conn.Login(config.Username, config.Password); err != nil {
			_ = conn.Quit()
			return nil, err
		}
	}

	return &ftpBucket{
		conn:   conn,
		config: config,
	}, nil
}

func (b *ftpBucket) stripPrefix(name string) string {
	if b.config.Prefix == "" {
		return name
	}
	name = strings.TrimPrefix(name, b.config.Prefix)
	name = strings.TrimPrefix(name, "/")
	return name
}

func (b *ftpBucket) withPrefix(name string) string {
	if b.config.Prefix == "" {
		return name
	}
	return internal.WithinNamespace(b.config.Prefix, name)
}

// Glob implements bfs.Bucket.
func (b *ftpBucket) Glob(ctx context.Context, pattern string) (bfs.Iterator, error) {
	// quick sanity check
	if _, err := doublestar.Match(pattern, ""); err != nil {
		return nil, err
	}

	files, subdirs, err := b.globDir(ctx, pattern, "", nil)
	if err != nil {
		return nil, err
	}

	iter := &iterator{
		bucket:  b,
		pattern: pattern,
		ctx:     ctx,
	}
	iter.reset(files, subdirs)
	return iter, nil
}

// Head implements bfs.Bucket.
func (b *ftpBucket) Head(_ context.Context, name string) (*bfs.MetaInfo, error) {
	entries, err := b.conn.List(b.withPrefix(name))
	if err != nil {
		return nil, normError(err)
	}

	if len(entries) != 1 || entries[0].Type != ftp.EntryTypeFile {
		return nil, bfs.ErrNotFound
	}

	return &bfs.MetaInfo{
		Name:    name,
		Size:    int64(entries[0].Size),
		ModTime: entries[0].Time,
	}, nil
}

// Open implements bfs.Bucket.
func (b *ftpBucket) Open(_ context.Context, name string) (io.ReadCloser, error) {
	rc, err := b.conn.Retr(b.withPrefix(name))
	if err != nil {
		return nil, normError(err)
	}
	return rc, nil
}

// Create implements bfs.Bucket.
func (b *ftpBucket) Create(ctx context.Context, name string, opts *bfs.WriteOptions) (io.WriteCloser, error) {
	f, err := ioutil.TempFile("", "bfs-ftp")
	if err != nil {
		return nil, err
	}

	return &writer{
		File:   f,
		bucket: b,
		ctx:    ctx,
		name:   name,
		opts:   opts,
	}, nil
}

// Remove implements bfs.Bucket.
func (b *ftpBucket) Remove(_ context.Context, name string) error {
	err := normError(b.conn.Delete(b.withPrefix(name)))
	if err != nil && err != bfs.ErrNotFound {
		return err
	}
	return nil
}

// Close implements bfs.Bucket.
func (b *ftpBucket) Close() error {
	return b.conn.Quit()
}

func (b *ftpBucket) mkdir(dir string) error {
	err := normError(b.conn.MakeDir(dir))
	if err != nil && err != bfs.ErrNotFound {
		return err
	}
	return nil
}

func (b *ftpBucket) mkdirAll(dir string) error {
	off := 0
	for {
		n := strings.IndexByte(dir[off:], '/')
		if n < 0 {
			break
		}
		if err := b.mkdir(dir[:off+n]); err != nil {
			return err
		}
		off += n + 1
	}
	return b.mkdir(dir)
}

func (b *ftpBucket) globDir(ctx context.Context, pattern string, dir string, subdirs []string) ([]*ftp.Entry, []string, error) {
	dir = strings.TrimRight(dir, "/")

	// get entries
	entries, err := b.conn.List(b.withPrefix(dir))
	if err != nil && normError(err) != bfs.ErrNotFound {
		return nil, subdirs, err
	}

	// split into files and dir subdirs
	files := entries[:0]
	for _, ent := range entries {

		ent.Name = path.Join(dir, ent.Name)
		if ent.Type == ftp.EntryTypeFile {
			if ok, err := doublestar.Match(pattern, ent.Name); err != nil {
				return nil, subdirs, err
			} else if ok {
				files = append(files, ent)
			}
		} else if ent.Type == ftp.EntryTypeFolder {
			subdirs = append(subdirs, ent.Name)
		}
	}
	return files, subdirs, nil
}

// --------------------------------------------------------

func normError(err error) error {
	if err == nil {
		return nil
	}

	switch err := err.(type) {
	case *textproto.Error:
		if err.Code == ftp.StatusFileUnavailable {
			return bfs.ErrNotFound
		}
	}
	return err
}

type writer struct {
	*os.File

	ctx    context.Context
	bucket *ftpBucket
	name   string
	opts   *bfs.WriteOptions

	closeOnce sync.Once
}

func (w *writer) Close() (err error) {
	w.closeOnce.Do(func() {
		// delete tempfile in the end
		fname := w.Name()
		defer os.Remove(fname)

		// close tempfile
		if err2 := w.File.Close(); err2 != nil {
			err = err2
			return
		}

		// check context
		if err2 := w.ctx.Err(); err2 != nil {
			err = err2
			return
		}

		// reopen for reading
		file, err2 := os.Open(fname)
		if err2 != nil {
			err = err2
			return
		}
		defer file.Close()

		fullName := w.bucket.withPrefix(w.name)
		if err2 := w.bucket.mkdirAll(path.Dir(fullName)); err2 != nil {
			err = err2
			return
		}

		err = w.bucket.conn.Stor(fullName, file)
	})
	return
}

// --------------------------------------------------------------------

type iterator struct {
	bucket  *ftpBucket
	pattern string
	files   []*ftp.Entry
	subdirs []string

	ctx context.Context
	err error
	pos int
}

func (i *iterator) Close() error {
	i.files = i.files[:0]
	i.subdirs = i.subdirs[:0]
	return nil
}

func (i *iterator) Name() string {
	if i.pos < len(i.files) {
		return i.files[i.pos].Name
	}
	return ""
}

func (i *iterator) Size() int64 {
	if i.pos < len(i.files) {
		return int64(i.files[i.pos].Size)
	}
	return 0
}

func (i *iterator) ModTime() time.Time {
	if i.pos < len(i.files) {
		return i.files[i.pos].Time
	}
	return time.Time{}
}

func (i *iterator) Next() bool {
	if i.err != nil {
		return false
	}

	if err := i.ctx.Err(); err != nil {
		i.err = err
		return false
	}

	if i.pos++; i.pos < len(i.files) {
		return true
	}

	if len(i.subdirs) == 0 {
		return false
	}

	// pop last dir
	tail := i.subdirs[len(i.subdirs)-1]
	i.subdirs = i.subdirs[:len(i.subdirs)-1]

	// glob dir
	files, subdirs, err := i.bucket.globDir(i.ctx, i.pattern, tail, i.subdirs)
	if err != nil {
		i.err = err
		return false
	}

	i.reset(files, subdirs)
	return i.Next()
}

func (i *iterator) reset(files []*ftp.Entry, subdirs []string) {
	i.pos = -1
	i.files = files
	i.subdirs = subdirs
}

func (i *iterator) Error() error { return i.err }
