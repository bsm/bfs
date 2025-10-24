package bfs_test

import (
	"testing"

	"github.com/bsm/bfs"
	"github.com/bsm/bfs/testdata/lint"
)

func TestInMem(t *testing.T) {
	bucket := bfs.NewInMem()
	support := lint.Supports{Metadata: true}
	lint.Common(t, bucket, support)
	lint.Slow(t, bucket, support)
}
