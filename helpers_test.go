package bfs_test

import (
	"reflect"
	"testing"

	"github.com/bsm/bfs"
)

func TestWriteObject(t *testing.T) {
	ctx := t.Context()
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
	ctx := t.Context()
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

func TestRemoveAll(t *testing.T) {
	ctx := t.Context()
	bucket := bfs.NewInMem()

	if err := bfs.WriteObject(ctx, bucket, "a/b.txt", []byte("testdata"), nil); err != nil {
		t.Fatal("Unexpected error", err)
	}
	if err := bfs.WriteObject(ctx, bucket, "a/b/c.txt", []byte("testdata"), nil); err != nil {
		t.Fatal("Unexpected error", err)
	}
	if err := bfs.WriteObject(ctx, bucket, "d.txt", []byte("testdata"), nil); err != nil {
		t.Fatal("Unexpected error", err)
	}
	if err := bfs.WriteObject(ctx, bucket, "e/f.txt", []byte("testdata"), nil); err != nil {
		t.Fatal("Unexpected error", err)
	}
	if got := bucket.ObjectSizes(); len(got) != 4 {
		t.Errorf("Expected %d items, got %#v", 4, got)
	}

	if err := bfs.RemoveAll(ctx, bucket, "a/**"); err != nil {
		t.Fatal("Unexpected error", err)
	}
	exp := map[string]int64{"d.txt": 8, "e/f.txt": 8}
	if got := bucket.ObjectSizes(); !reflect.DeepEqual(exp, got) {
		t.Errorf("Expected %#v, got %#v", exp, got)
	}

	if err := bfs.RemoveAll(ctx, bucket, "**"); err != nil {
		t.Fatal("Unexpected error", err)
	}
	if got := bucket.ObjectSizes(); len(got) != 0 {
		t.Errorf("Expected %d items, got %#v", 0, got)
	}
}
