package bfs

import (
	"context"
	"io"
	"net/url"
	"path"
	"strings"
)

// ObjectHandle is an abstract object handle.
type ObjectHandle interface {
	// Head returns an object's meta info.
	Head(context.Context) (*MetaInfo, error)
	// Open opens an object for reading.
	Open(context.Context) (io.ReadCloser, error)
	// Create creates/opens a object for writing.
	Create(context.Context) (io.WriteCloser, error)
	// Remove removes a object.
	Remove(context.Context) error
	// Close closes the object.
	Close() error
}

// Object is a handle for a single file/object on a Bucket.
type Object struct {
	name   string
	bucket Bucket
}

// NewObject inits a new object from an URL string
func NewObject(ctx context.Context, fullURL string) (*Object, error) {
	// parse URL
	u, err := url.Parse(fullURL)
	if err != nil {
		return nil, err
	}

	// store full path name and unset
	name := strings.TrimPrefix(path.Clean(u.Path), "/")
	u.Path = ""

	// resolve bucket
	bucket, err := Resolve(ctx, u)
	if err != nil {
		return nil, err
	}

	return &Object{
		name:   name,
		bucket: bucket,
	}, nil
}

// Head returns an object's meta info.
func (o *Object) Head(ctx context.Context) (*MetaInfo, error) {
	return o.bucket.Head(ctx, o.name)
}

// Open opens an object for reading.
func (o *Object) Open(ctx context.Context) (io.ReadCloser, error) {
	return o.bucket.Open(ctx, o.name)
}

// Create creates/opens a object for writing.
func (o *Object) Create(ctx context.Context) (io.WriteCloser, error) {
	return o.bucket.Create(ctx, o.name)
}

// Remove removes a object.
func (o *Object) Remove(ctx context.Context) error {
	return o.bucket.Remove(ctx, o.name)
}

// Close closes the object.
func (o *Object) Close() error {
	return o.bucket.Close()
}
