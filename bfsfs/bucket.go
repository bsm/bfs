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
	fsRoot string
	root   string
	tmpDir string
}

// New initiates an bfs.Bucket backed by local file system.
// tmpDir is used for file atomicity, defaults to standard tmp dir if blank.
func New(root, tmpDir string) (bfs.Bucket, error) {
	if root == "" {
		root = "."
	}
	root = filepath.Clean(root)

	sep := string(filepath.Separator)
	return &bucket{
		fsRoot: strings.TrimRight(root, sep) + sep, // root should always have trailing slash to trim file names properly
		root:   filepath.ToSlash(root),
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
			files = append(files, b.cleanPath(match))
		}
	}
	return newIterator(files), nil
}

// Head implements bfs.Bucket
func (b *bucket) Head(ctx context.Context, name string) (*bfs.MetaInfo, error) {
	fi, err := os.Stat(b.fullPath(name))
	if err != nil {
		return nil, normError(err)
	}

	return &bfs.MetaInfo{
		Name:    internal.NormObjectName(name),
		Size:    fi.Size(),
		ModTime: fi.ModTime(),
	}, nil
}

// Open implements bfs.Bucket
func (b *bucket) Open(ctx context.Context, name string) (io.ReadCloser, error) {
	f, err := os.Open(b.fullPath(name))
	if err != nil {
		return nil, normError(err)
	}
	return f, nil
}

// Create implements bfs.Bucket
func (b *bucket) Create(ctx context.Context, name string) (io.WriteCloser, error) {
	f, err := openAtomicFile(ctx, b.fullPath(name), b.tmpDir)
	if err != nil {
		return nil, normError(err)
	}
	return f, nil
}

// Remove implements bfs.Bucket
func (b *bucket) Remove(ctx context.Context, name string) error {
	err := os.Remove(b.fullPath(name))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Close implements bfs.Bucket
func (*bucket) Close() error {
	return nil // noop
}

func (b *bucket) fullPath(name string) string {
	return filepath.FromSlash(internal.WithinNamespace(b.root, filepath.ToSlash(name)))
}

func (b *bucket) cleanPath(name string) string {
	return internal.NormObjectName(strings.TrimPrefix(name, b.fsRoot))
}
