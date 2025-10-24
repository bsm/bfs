package bfs

import (
	"context"
	"io"
)

type supportsCopy interface {
	Copy(context.Context, string, string) error
}

type supportsRemoveAll interface {
	RemoveAll(context.Context, string) error
}

// WriteObject is a quick write helper.
func WriteObject(ctx context.Context, bucket Bucket, name string, data []byte, opts *WriteOptions) error {
	w, err := bucket.Create(ctx, name, opts)
	if err != nil {
		return err
	}
	defer w.Discard()

	if _, err := w.Write(data); err != nil {
		return err
	}
	return w.Commit()
}

// CopyObject is a quick helper to copy objects within the same bucket.
func CopyObject(ctx context.Context, bucket Bucket, src, dst string, dstOpts *WriteOptions) error {
	if b, ok := bucket.(supportsCopy); ok {
		return b.Copy(ctx, src, dst)
	}

	r, err := bucket.Open(ctx, src)
	if err != nil {
		return err
	}
	defer r.Close()

	w, err := bucket.Create(ctx, dst, dstOpts)
	if err != nil {
		return err
	}
	defer w.Discard()

	if _, err := io.Copy(w, r); err != nil {
		return err
	}
	return w.Commit()
}

// RemoveAll removes all files matching the pattern.
func RemoveAll(ctx context.Context, bucket Bucket, pattern string) error {
	if b, ok := bucket.(supportsRemoveAll); ok {
		return b.RemoveAll(ctx, pattern)
	}

	it, err := bucket.Glob(ctx, pattern)
	if err != nil {
		return err
	}
	defer it.Close()

	for it.Next() {
		if err := bucket.Remove(ctx, it.Name()); err != nil {
			return err
		}
	}
	return it.Error()
}
