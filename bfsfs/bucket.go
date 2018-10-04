package bfsfs

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/bmatcuk/doublestar"
	"github.com/bsm/bfs"
	"github.com/bsm/bfs/internal"
)

// bucket emulates bfs.Bucket behaviour for local file system.
type bucket struct {
	root   string
	tmpDir string
}

// New initiates an bfs.Bucket backed by local file system.
// tmpDir is used for file atomicity, defaults to standard tmp dir if blank.
func New(root, tmpDir string) (bfs.Bucket, error) {
	if root == "" {
		root = "."
	}

	return &bucket{
		root:   filepath.FromSlash(filepath.Clean(root)) + string(filepath.Separator), // root should always have trailing slash to trim file names properly
		tmpDir: tmpDir,
	}, nil
}

// Glob lists the files mathing a glob pattern.
func (b *bucket) Glob(_ context.Context, pattern string) (bfs.Iterator, error) {
	if pattern == "" { // would return just current dir
		return newIterator(nil), nil
	}

	matches, err := doublestar.Glob(b.fullPath(pattern))
	if err != nil {
		return nil, normError(err)
	}

	files := matches[:0]
	for _, match := range matches {
		if fi, err := os.Stat(match); err != nil {
			return nil, normError(err)
		} else if fi.Mode().IsRegular() {
			files = append(files, strings.TrimPrefix(match, b.root))
		}
	}
	return newIterator(files), nil
}

// Head returns an object's meta Info.
func (b *bucket) Head(ctx context.Context, name string) (*bfs.MetaInfo, error) {
	fi, err := os.Stat(b.fullPath(name))
	if err != nil {
		return nil, normError(err)
	}
	return &bfs.MetaInfo{
		Name:    name,
		Size:    fi.Size(),
		ModTime: fi.ModTime(),
	}, nil
}

// Open opens an object for reading.
func (b *bucket) Open(ctx context.Context, name string) (io.ReadCloser, error) {
	f, err := os.Open(b.fullPath(name))
	if err != nil {
		return nil, normError(err)
	}
	return f, nil
}

// Create creates/opens a object for writing.
func (b *bucket) Create(ctx context.Context, name string) (io.WriteCloser, error) {
	f, err := openAtomicFile(ctx, b.fullPath(name), b.tmpDir)
	if err != nil {
		return nil, normError(err)
	}
	return f, nil
}

// Remove removes a object.
func (b *bucket) Remove(ctx context.Context, name string) error {
	err := os.Remove(b.fullPath(name))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Close closes the bucket.
func (b *bucket) Close() error {
	return nil // noop
}

// Close closes the bucket.
func (b *bucket) fullPath(name string) string {
	return filepath.FromSlash(internal.WithinNamespace(filepath.ToSlash(b.root), filepath.ToSlash(name)))
}
