package bfsa

import (
	"context"

	"github.com/bsm/bfs"
)

type Resolver func(ctx context.Context, uri string) (
	bucket bfs.Bucket,
	path string,
	toURI func(name string) (uri string),
	err error,
)
