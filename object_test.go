package bfs_test

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/bsm/bfs"
)

func TestObject(t *testing.T) {
	ctx := context.Background()
	obj, err := bfs.NewObject(ctx, "mem:///path/to/file.txt")
	if err != nil {
		t.Fatal("Unexpected error", err)
	}

	t.Run("name", func(t *testing.T) {
		if exp, got := "path/to/file.txt", obj.Name(); exp != got {
			t.Errorf("Expected %q, got %q", exp, got)
		}
	})

	t.Run("not found", func(t *testing.T) {
		if _, err := obj.Head(ctx); !errors.Is(err, bfs.ErrNotFound) {
			t.Errorf("Expected %v, got %v", bfs.ErrNotFound, err)
		}

		if _, err := obj.Open(ctx); !errors.Is(err, bfs.ErrNotFound) {
			t.Errorf("Expected %v, got %v", bfs.ErrNotFound, err)
		}
	})

	t.Run("CRUD", func(t *testing.T) {
		w, err := obj.Create(ctx, nil)
		if err != nil {
			t.Fatal("Unexpected error", err)
		}
		defer w.Discard()

		if n, err := w.Write([]byte("TESTDATA")); err != nil {
			t.Fatal("Unexpected error", err)
		} else if n != 8 {
			t.Errorf("Expected %v, got %v", 8, n)
		}

		if err := w.Commit(); err != nil {
			t.Fatal("Unexpected error", err)
		}

		if i, err := obj.Head(ctx); err != nil {
			t.Fatal("Unexpected error", err)
		} else if i.Size != 8 {
			t.Errorf("Expected %v, got %v", 8, i.Size)
		} else if i.Name != obj.Name() {
			t.Errorf("Expected %v, got %v", obj.Name(), i.Name)
		}

		r, err := obj.Open(ctx)
		if err != nil {
			t.Fatal("Unexpected error", err)
		}
		defer r.Close()

		if data, err := io.ReadAll(r); err != nil {
			t.Fatal("Unexpected error", err)
		} else if exp, got := "TESTDATA", string(data); exp != got {
			t.Errorf("Expected %q, got %q", exp, got)
		}

		if err := r.Close(); err != nil {
			t.Fatal("Unexpected error", err)
		}

		if err := obj.Remove(ctx); err != nil {
			t.Fatal("Unexpected error", err)
		}

		if _, err := obj.Head(ctx); !errors.Is(err, bfs.ErrNotFound) {
			t.Errorf("Expected %v, got %v", bfs.ErrNotFound, err)
		}
	})
}
