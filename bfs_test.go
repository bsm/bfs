package bfs_test

import (
	"context"
	"net/url"

	"github.com/bsm/bfs"
)

// ------------------------------------------------------------------------

func init() {
	bfs.Register("mem", func(_ context.Context, u *url.URL) (bfs.Bucket, error) {
		return bfs.NewInMem(), nil
	})
}
