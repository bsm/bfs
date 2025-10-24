package bfs_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/bsm/bfs"
)

func TestWriteObject(t *testing.T) {
	ctx := context.Background()
	bucket := bfs.NewInMem()

	if err := bfs.WriteObject(ctx, bucket, "path/to/file", []byte("testdata"), nil); err != nil {
		t.Fatal("Unexpected error", err)
	}

	exp := map[string]int64{"path/to/file": 8}
	if got := bucket.ObjectSizes(); !reflect.DeepEqual(exp, got) {
		t.Errorf("Expected %#v, got %#v", exp, got)
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

	exp := map[string]int64{"src.txt": 8, "dst.txt": 8}
	if got := bucket.ObjectSizes(); !reflect.DeepEqual(exp, got) {
		t.Errorf("Expected %#v, got %#v", exp, got)
	}
}
