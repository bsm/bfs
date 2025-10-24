package bfsfs

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"

	"github.com/bmatcuk/doublestar/v4"
	"github.com/bsm/bfs"
)

// bucket emulates bfs.Bucket behaviour for local file system.
type bucket struct {
	root *os.Root
	// root   string
	tmpDir string
}

// New initiates an bfs.Bucket backed by local file system.
// tmpDir is used for file atomicity, defaults to standard tmp dir if blank.
func New(root, tmpDir string) (bfs.Bucket, error) {
	if root == "" {
		root = "."
	}
	root = filepath.Clean(root)

	rootFS, err := os.OpenRoot(root)
	if err != nil {
		return nil, err
	}

	return &bucket{
		root:   rootFS, // root should always have trailing slash to trim file names properly
		tmpDir: tmpDir,
	}, nil
}

// Glob lists the files matching a glob pattern.
func (b *bucket) Glob(ctx context.Context, pattern string) (bfs.Iterator, error) {
	if pattern == "" { // would return just current dir
		return newIterator(nil), nil
	}

	files := make([]file, 0)
	err := doublestar.GlobWalk(b.root.FS(), pattern, func(match string, d fs.DirEntry) error {
		fi, err := b.root.Stat(match)
		if err != nil {
			return normError(err)
		}

		files = append(files, file{
			name:    filepath.ToSlash(match),
			size:    fi.Size(),
			modTime: fi.ModTime(),
		})
		return nil
	}, doublestar.WithFilesOnly(), doublestar.WithNoFollow())
	if err != nil {
		return nil, normError(err)
	}

	return newIterator(files), nil
}

// Head implements bfs.Bucket
func (b *bucket) Head(ctx context.Context, name string) (*bfs.MetaInfo, error) {
	fi, err := b.root.Stat(filepath.FromSlash(name))
	if err != nil {
		return nil, normError(err)
	} else if !fi.Mode().IsRegular() {
		return nil, bfs.ErrNotFound
	}

	return &bfs.MetaInfo{
		Name:    name,
		Size:    fi.Size(),
		ModTime: fi.ModTime(),
	}, nil
}

// Open implements bfs.Bucket
func (b *bucket) Open(ctx context.Context, name string) (bfs.Reader, error) {
	f, err := b.root.Open(filepath.FromSlash(name))
	if err != nil {
		return nil, normError(err)
	}

	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, normError(err)
	} else if !fi.Mode().IsRegular() {
		_ = f.Close()
		return nil, bfs.ErrNotFound
	}

	return f, nil
}

// Create implements bfs.Bucket
func (b *bucket) Create(ctx context.Context, name string, _ *bfs.WriteOptions) (bfs.Writer, error) {
	f, err := openAtomicFile(ctx, b.root, filepath.FromSlash(name), b.tmpDir)
	if err != nil {
		return nil, normError(err)
	}
	return f, nil
}

// Remove implements bfs.Bucket
func (b *bucket) Remove(ctx context.Context, name string) error {
	err := b.root.Remove(filepath.FromSlash(name))
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		if pe := new(fs.PathError); !errors.As(err, &pe) || pe.Err != syscall.ENOTEMPTY {
			return err
		}
	}
	return nil
}

// RemoveAll removes all files matching a glob pattern.
func (b *bucket) RemoveAll(ctx context.Context, pattern string) error {
	if pattern == "" { // does not delete anything
		return nil
	}

	err := doublestar.GlobWalk(b.root.FS(), pattern, func(path string, d fs.DirEntry) error {
		return b.root.Remove(path)
	}, doublestar.WithFilesOnly(), doublestar.WithNoFollow())
	if err != nil {
		return normError(err)
	}
	return nil
}

// Close implements bfs.Bucket
func (b *bucket) Close() error {
	return nil // noop
}
