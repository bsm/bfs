// Package bfsfs abstracts local file system.
//
// When imported, it registers a global `file://` scheme resolver and can be used like:
//
//   import (
//     "github.com/bsm/bfs"
//
//     _ "github.com/bsm/bfs/bfsfs"
//   )
//
//   func main() {
//     ctx := context.Background()
//
//     u, _ := url.Parse("file://path/to/file.ext?tmpdir=path/to/tmp/dir")
//     bucket, _ := bfs.Resolve(ctx, u)
//
//     f, _ := bucket.Open(ctx, u.Path)
//     ...
//   }
//
package bfsfs

import (
	"context"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"

	"github.com/bsm/bfs"
)

func init() {
	bfs.Register("file", func(_ context.Context, u *url.URL) (bfs.Bucket, error) {
		q := u.Query()
		return New(u.Host, q.Get("tmpdir"))
	})
}

// normError normalizes error.
func normError(err error) error {
	switch {
	case err == nil:
		return nil
	case os.IsNotExist(err):
		return bfs.ErrNotFound
	default:
		return err
	}
}

// --------------------------------------------------------------------

// atomicFile represents a file, that's written only on Close.
type atomicFile struct {
	*os.File

	ctx  context.Context
	name string
}

// openAtomicFile opens atomic file for writing.
// tmpDir defaults to standard temporary dir if blank.
func openAtomicFile(ctx context.Context, name string, tmpDir string) (*atomicFile, error) {
	f, err := ioutil.TempFile(tmpDir, "bfsfs")
	if err != nil {
		return nil, err
	}
	return &atomicFile{
		File: f,
		ctx:  ctx,
		name: name,
	}, nil
}

// Close commits the file.
func (f *atomicFile) Close() error {
	defer f.cleanup()

	if err := f.File.Close(); err != nil {
		return err
	}

	select {
	case <-f.ctx.Done():
		return f.ctx.Err()
	default:
	}

	if err := os.MkdirAll(filepath.Dir(f.name), 0777); err != nil {
		return err
	}

	return os.Rename(f.Name(), f.name)
}

// cleanup removes temporary file.
func (f *atomicFile) cleanup() {
	_ = os.Remove(f.Name())
}
