package bfs_test

import (
	"context"
	"testing"

	"github.com/bsm/bfs"
)

func TestWriteObject(t *testing.T) {
	ctx := context.Background()
	bucket := bfs.NewInMem()

	if err := bfs.WriteObject(ctx, bucket, "path/to/file", []byte("testdata"), nil); err != nil {
		t.Fatal("Unexpected error", err)
	}

	if omap := bucket.ObjectSizes(); len(omap) != 1 {
		t.Errorf("Expected %#v to have 1 entry", omap)
	} else if omap["path/to/file"] != 8 {
		t.Errorf("Expected %#v to include %q: %d", omap, "path/to/file", 8)
	}
}

func TestCopyObject(t *testing.T) {
	ctx := context.Background()
	bucket := bfs.NewInMem()

	if err := bfs.WriteObject(ctx, bucket, "src.txt", []byte("testdata"), nil); err != nil {
		t.Fatal("Unexpected error", err)
	}
	if err := bfs.CopyObject(ctx, bucket, "src.txt", "dst.txt", nil); err != nil {
		t.Fatal("Unexpected error", err)
	}

	if omap := bucket.ObjectSizes(); len(omap) != 2 {
		t.Errorf("Expected %#v to have 2 entries", omap)
	} else if omap["src.txt"] != 8 {
		t.Errorf("Expected %#v to include %q: %d", omap, "src.txt", 8)
	} else if omap["dst.txt"] != 8 {
		t.Errorf("Expected %#v to include %q: %d", omap, "dst.txt", 8)
	}
}
