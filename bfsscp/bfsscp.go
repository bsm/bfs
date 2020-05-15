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
//     b, _ := bfs.Connect(ctx, "ssh://user:pass@hostname:22/path/to/root?tmpdir=%2Fcustom%2Ftmp")
//     f, _ := b.Open(ctx, "file/within/root.txt")
//     ...
//   }
//
// bfs.Connect supports the following query parameters:
//
//   tmpdir - custom temp dir
//
package bfsscp

import (
	"bytes"
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

// Register allows for registering more schemes with SCP
func Register(_ context.Context, u *url.URL) (bfs.Bucket, error) {
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
}

func init() {
	bfs.Register("ssh", Register)
	bfs.Register("scp", Register)
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
}

func (c *Config) norm() error {
	c.Prefix = strings.TrimPrefix(c.Prefix, "/")
	if c.Prefix != "" && !strings.HasSuffix(c.Prefix, "/") {
		c.Prefix = c.Prefix + "/"
	}
	return nil
}

type bucket struct {
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
func (b *bucket) Glob(_ context.Context, pattern string) (bfs.Iterator, error) {
	// quick sanity check
	if _, err := doublestar.Match(pattern, ""); err != nil {
		return nil, err
	}

	// create a new iterator
	return &iterator{
		w:       b.client.Walk(b.withPrefix("/")),
		pattern: b.withPrefix(pattern),
	}, nil
}

// Head implements bfs.Bucket.
func (b *bucket) Head(_ context.Context, name string) (*bfs.MetaInfo, error) {
	fileInfo, err := b.client.Stat(b.withPrefix(name))
	if err != nil {
		return nil, normError(err)
	}

	return &bfs.MetaInfo{
		Name:    name,
		Size:    fileInfo.Size(),
		ModTime: fileInfo.ModTime(),
	}, nil
}

// Open implements bfs.Bucket.
func (b *bucket) Open(_ context.Context, name string) (bfs.Reader, error) {
	f, err := b.client.Open(b.withPrefix(name))
	if err != nil {
		return nil, normError(err)
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, normError(err)
	}

	// Use file size to ensure limited read and avoid EOFs
	return &readerCloser{r: f, size: fi.Size()}, nil
}

// Create implements bfs.Bucket.
func (b *bucket) Create(ctx context.Context, name string, opts *bfs.WriteOptions) (bfs.Writer, error) {
	f, err := ioutil.TempFile(b.config.TempDir, "bfs-scp")
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
func (b *bucket) Remove(_ context.Context, name string) error {
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
	*os.File

	ctx    context.Context
	bucket *bucket
	name   string
	opts   *bfs.WriteOptions

	closeOnce sync.Once
}

func (w *writer) Discard() error {
	err := os.ErrClosed
	w.closeOnce.Do(func() {
		// delete tempfile in the end
		fname := w.Name()
		defer os.Remove(fname)

		// close tempfile
		err = w.File.Close()
	})
	return err
}

func (w *writer) Commit() error {
	err := os.ErrClosed
	w.closeOnce.Do(func() {
		// delete tempfile in the end
		fname := w.Name()
		defer os.Remove(fname)

		// close tempfile, check context
		if err = w.File.Close(); err != nil {
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
		sf, err = w.bucket.client.Create(fullName)
		if err != nil {
			return
		}
		_, err = io.Copy(sf, file)
		return
	})
	return err
}

// --------------------------------------------------------

type readerCloser struct {
	r    *sftp.File
	size int64
}

func (rc *readerCloser) Read(out []byte) (int, error) {
	buf := make([]byte, rc.size)
	if _, err := rc.r.Read(buf); err != nil {
		return 0, err
	}

	b := bytes.NewBuffer(buf)
	return b.Read(out)
}

func (rc *readerCloser) Close() error {
	return rc.r.Close()
}

// --------------------------------------------------------

type iterator struct {
	w       *fs.Walker
	pattern string
	file    os.FileInfo
	err     error
}

// Next advances the cursor to the next position.
func (it *iterator) Next() bool {
	it.file = nil

	if it.w.Step() {
		it.file = it.w.Stat()
		if it.file == nil || it.file.IsDir() {
			return it.Next()
		}

		if match, err := doublestar.Match(it.pattern, it.w.Path()); err != nil {
			it.err = err
			return false
		} else if match {
			return true
		}

		return it.Next()
	}

	it.err = it.w.Err()
	return false
}

// Name returns the name at the current cursor position.
func (it *iterator) Name() string {
	if f := it.file; f != nil {
		return f.Name()
	}
	return ""
}

// Size returns the content length in bytes at the current cursor position.
func (it *iterator) Size() int64 {
	if f := it.file; f != nil {
		return f.Size()
	}
	return 0
}

// ModTime returns the modification time at the current cursor position.
func (it *iterator) ModTime() time.Time {
	if f := it.file; f != nil {
		return f.ModTime()
	}
	return time.Time{}
}

// Error returns the last iterator error, if any.
func (it *iterator) Error() error {
	return it.err
}

// Close closes the iterator, should always be deferred.
func (it *iterator) Close() error {
	it.file = nil
	return nil
}

// --------------------------------------------------------

func normError(err error) error {
	if err == nil {
		return nil
	}

	switch err {
	case os.ErrNotExist:
		return bfs.ErrNotFound
	}
	return err
}
