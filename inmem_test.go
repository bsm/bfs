package bfs_test

import (
	"testing"

	"github.com/bsm/bfs"
	"github.com/bsm/bfs/testdata/lint"
)

func TestInMem(t *testing.T) {
	bucket := bfs.NewInMem()
	lint.Common(t, bucket, lint.Supports{Metadata: true})
}
