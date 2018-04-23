// Package bfs outlines an abstraction for bucket-based fyle systems with
// mock-implmentations.
package bfs

import (
	"context"
	"errors"
	"io"
	"time"
)

// ErrNotFound must be returned by all implementations
// when a requested object cannot be found.
var ErrNotFound = errors.New("bfs: object not found")

// Bucket is an abstract storage bucket.
type Bucket interface {
	// Glob lists the files mathing a glob pattern.
	Glob(ctx context.Context, pattern string) (Iterator, error)

	// Head returns an object's meta Info.
	Head(ctx context.Context, name string) (*MetaInfo, error)

	// Open opens an object for reading.
	Open(ctx context.Context, name string) (io.ReadCloser, error)

	// Create creates/opens a object for writing.
	Create(ctx context.Context, name string) (io.WriteCloser, error)

	// Remove removes a object.
	Remove(ctx context.Context, name string) error

	// Close closes the bucket.
	Close() error
}

// MetaInfo contains meta information about an object.
type MetaInfo struct {
	Name    string    // base name of the object
	Size    int64     // length of the content in bytes
	ModTime time.Time // modification time
}

// Iterator iterates over objects
type Iterator interface {
	// Next advances the cursor to the next position.
	Next() bool
	// Name returns the name at the current cursor position.
	Name() string
	// Error returns the last iterator error, if any.
	Error() error
	// Close closes the iterator, should always be deferred.
	Close() error
}

type supportsCopying interface {
	Copy(context.Context, string, string) error
}
