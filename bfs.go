// Package bfs outlines an abstraction for bucket-based fyle systems with
// mock-implmentations.
package bfs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sync"
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

// --------------------------------------------------------------------

// Resolve opens a bucket from a URL string.
func Resolve(ctx context.Context, u *url.URL) (Bucket, error) {
	registryLock.Lock()
	resv, ok := registry[u.Scheme]
	registryLock.Unlock()
	if !ok {
		return nil, fmt.Errorf("bfs: unkown URL scheme %q", u.Scheme)
	}

	return resv(ctx, u)
}

// Resolver constructs a bucket from a URL.
type Resolver func(context.Context, *url.URL) (Bucket, error)

var (
	registry     = map[string]Resolver{}
	registryLock sync.Mutex
)

// RegisterProtocol registers a new protocol with a resolver.
func RegisterProtocol(scheme string, resv Resolver) {
	registryLock.Lock()
	defer registryLock.Unlock()

	if _, exists := registry[scheme]; exists {
		panic("protocol " + scheme + " already registered")
	}
	registry[scheme] = resv
}
