package bfs

import (
	"context"
	"fmt"
	"net/url"
	"path"
	"strings"
)

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
	if name == "." {
		return nil, fmt.Errorf("bfs: invalid URL path %q", u.Path)
	}
	u.Path = "/"

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

// NewInMemObject returns a new in-memory object.
func NewInMemObject(name string) *Object {
	return &Object{
		name:   name,
		bucket: NewInMem(),
	}
}

// Name returns an object's name.
func (o *Object) Name() string {
	return o.name
}

// Head returns an object's meta info.
func (o *Object) Head(ctx context.Context) (*MetaInfo, error) {
	return o.bucket.Head(ctx, o.name)
}

// Open opens an object for reading.
func (o *Object) Open(ctx context.Context) (Reader, error) {
	return o.bucket.Open(ctx, o.name)
}

// Create creates/opens a object for writing.
func (o *Object) Create(ctx context.Context, opts *WriteOptions) (Writer, error) {
	return o.bucket.Create(ctx, o.name, opts)
}

// Remove removes a object.
func (o *Object) Remove(ctx context.Context) error {
	return o.bucket.Remove(ctx, o.name)
}

// Close closes the object.
func (o *Object) Close() error {
	return o.bucket.Close()
}
