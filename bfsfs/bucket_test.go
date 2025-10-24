package bfsfs_test

import (
	"os"
	"testing"

	"github.com/bsm/bfs/bfsfs"
	"github.com/bsm/bfs/testdata/lint"
)

func Test(t *testing.T) {
	dir, err := os.MkdirTemp("", "bfsfs")
	if err != nil {
		t.Fatal("Unexpected error", err)
	}
	defer os.RemoveAll(dir)

	bucket, err := bfsfs.New(dir, "")
	if err != nil {
		t.Fatal("Unexpected error", err)
	}

	lint.Common(t, bucket, lint.Supports{})
}
