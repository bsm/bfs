package bfsa

import (
	"context"
	"io"

	"github.com/bsm/bfs"
)

type bucket struct {
	reg registry
}

func New(opts ...Option) (bfs.Bucket, error) {
	var b bucket
	for _, opt := range opts {
		if err := opt.setup(&b); err != nil {
			return nil, err
		}
	}
	return &b, nil
}

// Glob lists the files mathing a glob pattern.
func (b *bucket) Glob(ctx context.Context, pattern string) (bfs.Iterator, error) {
	ctx, cancel := context.WithCancel(ctx)

	bucket, path, toURI, err := b.resolve(ctx, pattern)
	if err != nil {
		cancel()
		return nil, err
	}

	it, err := bucket.Glob(ctx, path)
	if err != nil {
		cancel()
		return nil, err
	}

	return &iterator{
		Iterator: it,
		toURI:    toURI,
		closer: func(err error) error {
			_ = bucket.Close()
			cancel()
			return err
		},
	}, nil
}

// Head returns an object's meta Info.
func (b *bucket) Head(ctx context.Context, name string) (*bfs.MetaInfo, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	bucket, path, _, err := b.resolve(ctx, name)
	if err != nil {
		return nil, err
	}
	defer bucket.Close()

	return bucket.Head(ctx, path)
}

// Open opens an object for reading.
func (b *bucket) Open(ctx context.Context, name string) (io.ReadCloser, error) {
	ctx, cancel := context.WithCancel(ctx)

	bucket, path, _, err := b.resolve(ctx, name)
	if err != nil {
		cancel()
		return nil, err
	}

	rc, err := bucket.Open(ctx, path)
	if err != nil {
		cancel()
		return nil, err
	}

	return &readCloser{
		ReadCloser: rc,
		closer: func(err error) error {
			_ = bucket.Close()
			cancel()
			return err
		},
	}, nil
}

// Create creates/opens a object for writing.
func (b *bucket) Create(ctx context.Context, name string) (io.WriteCloser, error) {
	ctx, cancel := context.WithCancel(ctx)

	bucket, path, _, err := b.resolve(ctx, name)
	if err != nil {
		cancel()
		return nil, err
	}

	wc, err := bucket.Create(ctx, path)
	if err != nil {
		cancel()
		return nil, err
	}

	return &writeCloser{
		WriteCloser: wc,
		closer: func(err error) error {
			_ = bucket.Close()
			cancel()
			return err
		},
	}, nil
}

// Remove removes a object.
func (b *bucket) Remove(ctx context.Context, name string) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	bucket, path, _, err := b.resolve(ctx, name)
	if err != nil {
		return err
	}
	defer bucket.Close()

	return bucket.Remove(ctx, path)
}

// Close closes the bucket.
func (b *bucket) Close() error {
	return nil
}

func (b *bucket) resolve(ctx context.Context, uri string) (bfs.Bucket, string, func(string) string, error) {
	bucket, path, toURI, err := b.reg.resolve(ctx, uri)
	if _, ok := err.(unsupportedSchemeError); !ok {
		return bucket, path, toURI, err
	}
	return reg.resolve(ctx, uri)
}

var _ bfs.Bucket = &bucket{}
