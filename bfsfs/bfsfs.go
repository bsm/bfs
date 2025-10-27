// Package bfsfs abstracts local file system.
//
// When imported, it registers a global `file://` scheme resolver and can be used like:
//
//	import (
//	  "github.com/bsm/bfs"
//
//	  _ "github.com/bsm/bfs/bfsfs"
//	)
//
//	func main() {
//	  ctx := context.TODO()
//	  b, _ := bfs.Connect(ctx, "file:///path/to/root?tmpdir=%2Fcustom%2Ftmp")
//	  f, _ := b.Open(ctx, "file/within/root.txt")
//	  ...
//	}
//
// bfs.Connect supports the following query parameters:
//
//	tmpdir - custom temp dir
package bfsfs

import (
	"context"
	"net/url"
	"os"
	"path"
	"path/filepath"

	"github.com/bsm/bfs"
)

func init() {
	bfs.Register("file", func(_ context.Context, u *url.URL) (bfs.Bucket, error) {
		root := path.Join(u.Host, u.Path) // to handle special relative cases like: "file://this-works-like-a-host/path..."
		q := u.Query()
		return New(root, q.Get("tmpdir"))
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
	root *os.Root
	name string
}

// openAtomicFile opens atomic file for writing.
// tmpDir defaults to standard temporary dir if blank.
func openAtomicFile(ctx context.Context, root *os.Root, name, tmpDir string) (*atomicFile, error) {
	f, err := os.CreateTemp(tmpDir, "github_com__bsm__bfs__bfsfs")
	if err != nil {
		return nil, err
	}

	return &atomicFile{
		File: f,
		ctx:  ctx,
		root: root,
		name: name,
	}, nil
}

// Discard discards the file.
func (f *atomicFile) Discard() error {
	defer f.cleanup()

	return f.Close()
}

// Commit commits the file.
func (f *atomicFile) Commit() error {
	defer f.cleanup()

	if err := f.Close(); err != nil {
		return err
	}

	select {
	case <-f.ctx.Done():
		return f.ctx.Err()
	default:
	}

	if err := f.root.MkdirAll(filepath.Dir(f.name), 0777); err != nil {
		return err
	}

	return os.Rename(f.Name(), filepath.Join(f.root.Name(), path.Clean("/"+f.name)))
}

// cleanup removes temporary file.
func (f *atomicFile) cleanup() {
	_ = os.Remove(f.Name())
}
