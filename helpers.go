package bfs

import (
	"context"
	"io"
)

// WriteObject is a quick write helper.
func WriteObject(bucket Bucket, ctx context.Context, name string, data []byte, opts *WriteOptions) error {
	w, err := bucket.Create(ctx, name, opts)
	if err != nil {
		return err
	}
	defer w.Close()

	if _, err := w.Write(data); err != nil {
		return err
	}
	return w.Close()
}

// CopyObject is a quick helper to copy objects within the same bucket.
func CopyObject(bucket Bucket, ctx context.Context, src, dst string, dstOpts *WriteOptions) error {
	if cp, ok := bucket.(supportsCopying); ok {
		return cp.Copy(ctx, src, dst)
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
	defer w.Close()

	if _, err := io.Copy(w, r); err != nil {
		return err
	}
	return w.Close()
}
