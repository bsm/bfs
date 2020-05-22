// Package bfsscp abstracts an SSH/SCP workflow
//
// When imported, it registers both `scp://` and `ssh://` global scheme resolver and can be used like:
//
//   import (
//     "github.com/bsm/bfs"
//
//     _ "github.com/bsm/bfs/bfsscp"
//   )
//
//   func main() {
//     ctx := context.Background()
//     b, _ := bfs.Connect(ctx, "ssh://user:pass@hostname:22/path/to/root?tmpdir=%2Fcustom%2Ftmp&absolute=true")
//     f, _ := b.Open(ctx, "file/within/root.txt")
//     ...
//   }
//
// bfs.Connect supports the following query parameters:
//
//   tmpdir - custom temp dir
//   absolute - to keep the inital slash for prefix, making it an absolute path
//
package bfsscp

import (
	"context"
	"io"
	"io/ioutil"
	"net"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/bmatcuk/doublestar"
	"github.com/bsm/bfs"
	"github.com/bsm/bfs/internal"
	"github.com/kr/fs"
	"github.com/pkg/sftp"
	"go.uber.org/multierr"
	"golang.org/x/crypto/ssh"
)

func init() {
	bfs.Register("scp", func(_ context.Context, u *url.URL) (bfs.Bucket, error) {
		query := u.Query()
		address := net.JoinHostPort(u.Hostname(), u.Port())

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
			Absolute: query.Get("absolute") == "true",
		})
	})
}

// Config is passed to New to configure the SSH connection.
type Config struct {
	// Username to use.
	Username string
	// Password to use in combination with Username.
	Password string
	// An optional path prefix.
	Prefix string
	// A custom temp dir.
	TempDir string
	// Whether Prefix Paths are absolute or relative
	Absolute bool
}

func (c *Config) norm() error {
	// By default prefixes should start in the user's dir
	if !c.Absolute {
		c.Prefix = strings.TrimPrefix(c.Prefix, "/")
	}

	// Ensure even blank prefixes use slash when absolute path is prefered
	if c.Absolute && c.Prefix == "" {
		c.Prefix = "/"
	}

	// Add a trailing slash when one doesn't exist
	if c.Prefix != "" && !strings.HasSuffix(c.Prefix, "/") {
		c.Prefix = c.Prefix + "/"
	}
	return nil
}

type bucket struct {
	ctx    context.Context
	conn   *ssh.Client
	client *sftp.Client
	config *Config
}

// New initiates an bfs.Bucket backed by ssh.
func New(address string, cfg *Config) (bfs.Bucket, error) {
	config := new(Config)
	if cfg != nil {
		*config = *cfg
	}
	if err := config.norm(); err != nil {
		return nil, err
	}

	sshConfig := &ssh.ClientConfig{
		User: cfg.Username,
		Auth: []ssh.AuthMethod{
			ssh.Password(cfg.Password),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}

	conn, err := ssh.Dial("tcp", address, sshConfig)
	if err != nil {
		return nil, err
	}

	client, err := sftp.NewClient(conn)
	if err != nil {
		return nil, multierr.Combine(conn.Close(), err)
	}

	return &bucket{
		conn:   conn,
		client: client,
		config: config,
	}, nil
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

	// Need to walk the tree for global pattern
	if strings.Contains(pattern, "**") {
		// create a new walker iterator
		return &walkerIterator{
			w:            b.client.Walk(b.withPrefix("/")),
			infoIterator: infoIterator{ctx: ctx},
		}, nil
	}

	return newMatchesIterator(ctx, b.client, b.withPrefix(pattern))
}

// Head implements bfs.Bucket.
func (b *bucket) Head(ctx context.Context, name string) (*bfs.MetaInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	info, err := b.client.Stat(b.withPrefix(name))
	if err != nil {
		return nil, normError(err)
	}

	return &bfs.MetaInfo{
		Name:    name,
		Size:    info.Size(),
		ModTime: info.ModTime(),
	}, nil
}

// Open implements bfs.Bucket.
func (b *bucket) Open(ctx context.Context, name string) (bfs.Reader, error) {
	file, err := b.client.Open(b.withPrefix(name))
	if err != nil {
		return nil, normError(err)
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, normError(err)
	}

	// Use file size to ensure limited read and avoid EOFs
	return &reader{File: file, limited: io.LimitReader(file, info.Size()), ctx: ctx}, nil
}

// Create implements bfs.Bucket.
func (b *bucket) Create(ctx context.Context, name string, opts *bfs.WriteOptions) (bfs.Writer, error) {
	f, err := ioutil.TempFile(b.config.TempDir, "bfs-scp")
	if err != nil {
		return nil, err
	}

	return &writer{
		tmp:    f,
		bucket: b,
		ctx:    ctx,
		name:   name,
		opts:   opts,
	}, nil
}

// Remove implements bfs.Bucket.
func (b *bucket) Remove(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	err := normError(b.client.Remove(b.withPrefix(name)))
	if err != nil && err != bfs.ErrNotFound {
		return err
	}
	return nil
}

// Close implements bfs.Bucket.
func (b *bucket) Close() error {
	return multierr.Combine(b.client.Close(), b.conn.Close())
}

// --------------------------------------------------------

type writer struct {
	tmp    *os.File
	ctx    context.Context
	bucket *bucket
	name   string
	opts   *bfs.WriteOptions

	closeOnce sync.Once
}

func (w *writer) Write(data []byte) (int, error) {
	return w.tmp.Write(data)
}

func (w *writer) Discard() error {
	err := os.ErrClosed
	w.closeOnce.Do(func() {
		// delete tempfile in the end
		fname := w.tmp.Name()
		defer os.Remove(fname)

		// close tempfile
		err = w.tmp.Close()
	})
	return err
}

func (w *writer) Commit() error {
	err := os.ErrClosed
	w.closeOnce.Do(func() {
		// delete tempfile in the end
		fname := w.tmp.Name()
		defer os.Remove(fname)

		// close tempfile, check context
		if err = w.tmp.Close(); err != nil {
			return
		} else if err = w.ctx.Err(); err != nil {
			return
		}

		// reopen for reading
		var file *os.File
		if file, err = os.Open(fname); err != nil {
			return
		}
		defer file.Close()

		fullName := w.bucket.withPrefix(w.name)
		if err = w.bucket.client.MkdirAll(path.Dir(fullName)); err != nil {
			return
		}

		var sf *sftp.File
		if sf, err = w.bucket.client.Create(fullName); err != nil {
			return
		}
		_, err = io.Copy(sf, file)
		return
	})
	return err
}

// --------------------------------------------------------

type reader struct {
	*sftp.File
	limited io.Reader
	ctx     context.Context
}

func (r *reader) Read(out []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}

	return r.limited.Read(out)
}

// --------------------------------------------------------

type infoIterator struct {
	ctx  context.Context
	info os.FileInfo
	err  error
}

// Name returns the current name.
func (it *infoIterator) Name() string {
	if f := it.info; f != nil {
		return f.Name()
	}
	return ""
}

// Size returns the current content length in bytes.
func (it *infoIterator) Size() int64 {
	if f := it.info; f != nil {
		return f.Size()
	}
	return 0
}

// ModTime returns the current modification time.
func (it *infoIterator) ModTime() time.Time {
	if f := it.info; f != nil {
		return f.ModTime()
	}
	return time.Time{}
}

// Error returns the last iterator error, if any.
func (it *infoIterator) Error() error {
	return it.err
}

// Close closes the iterator, should always be deferred.
func (it *infoIterator) Close() error {
	it.info = nil
	return nil
}

// --------------------------------------------------------

type walkerIterator struct {
	infoIterator
	w *fs.Walker
}

// Next advances the cursor to the next position.
func (it *walkerIterator) Next() bool {
	if it.err != nil {
		return false
	}

	if err := it.ctx.Err(); err != nil {
		it.err = err
		return false
	}

	it.info = nil

	if it.w.Step() {
		it.info = it.w.Stat()
		if it.info == nil || it.info.IsDir() {
			return it.Next()
		}

		return true
	}

	it.err = it.w.Err()
	return false
}

// --------------------------------------------------------

type matchesIterator struct {
	infoIterator
	client  *sftp.Client
	matches []string
	pos     int
}

func newMatchesIterator(ctx context.Context, client *sftp.Client, pattern string) (*matchesIterator, error) {
	matches, err := client.Glob(pattern)
	if err != nil {
		return nil, err
	}

	return &matchesIterator{
		client:       client,
		matches:      matches,
		pos:          -1,
		infoIterator: infoIterator{ctx: ctx},
	}, nil
}

// Next advances the cursor to the next position.
func (it *matchesIterator) Next() bool {
	if it.err != nil {
		return false
	}

	if err := it.ctx.Err(); err != nil {
		it.err = err
		return false
	}

	it.pos++
	if it.pos >= len(it.matches) {
		return false
	}

	it.info, it.err = it.client.Stat(it.matches[it.pos])
	if it.err != nil {
		return false
	}
	if it.info == nil || it.info.IsDir() {
		return it.Next()
	}

	return true
}

// --------------------------------------------------------

func normError(err error) error {
	switch err {
	case os.ErrNotExist:
		return bfs.ErrNotFound
	case nil:
		return nil
	}
	return err
}
