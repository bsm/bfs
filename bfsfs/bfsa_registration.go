package bfsfs

import (
	"context"
	"strings"

	"github.com/bsm/bfs"
	"github.com/bsm/bfs/bfsa"
)

// DefaultScheme is a recommended scheme for local file system.
const DefaultScheme = "file://"

// Register registers abstract storage, backed by local file system with given root and tmpDir, for specified schema.
func Register(scheme, root, tmpDir string) {
	resolver := func(ctx context.Context, uri string) (
		bfs.Bucket,
		string,
		func(name string) (uri string),
		error,
	) {
		bucket, err := New(root, tmpDir)
		if err != nil {
			return nil, "", nil, err
		}

		path := strings.TrimPrefix(uri, scheme)
		return bucket, path, func(name string) string {
			return scheme + name
		}, nil
	}
	bfsa.Register(scheme, resolver)
}
