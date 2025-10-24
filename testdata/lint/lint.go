package lint

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/bsm/bfs"
)

type Supports struct {
	ContentType bool
	Metadata    bool
}

func Common(t *testing.T, bucket bfs.Bucket, supports Supports) {
	ctx := context.Background()

	t.Run("writes", func(t *testing.T) {
		w, err := bucket.Create(ctx, "blank.txt", nil)
		if err != nil {
			t.Fatal("Unexpected error", err)
		}
		defer w.Discard()

		assertNoError(t, w.Commit())

		if exp, got := []string{"blank.txt"}, glob(t, bucket, "*"); !reflect.DeepEqual(exp, got) {
			t.Errorf("Expected %v, got %v", exp, got)
		}

		assertError(t, w.Discard())
		assertNoError(t, bucket.Remove(ctx, "blank.txt"))
	})

	t.Run("aborts writes on discard", func(t *testing.T) {
		w, err := bucket.Create(ctx, "blank.txt", nil)
		assertNoError(t, err)
		defer w.Discard()

		assertNumEntries(t, bucket, "*", 0)

		assertNoError(t, w.Discard())
		assertError(t, w.Commit())
		assertNumEntries(t, bucket, "*", 0)
	})

	t.Run("aborts writes if context is cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()

		w, err := bucket.Create(ctx, "blank.txt", nil)
		assertNoError(t, err)
		defer w.Discard()

		assertNumEntries(t, bucket, "*", 0)
		cancel()

		if exp, got := context.Canceled, w.Commit(); !errors.Is(got, exp) {
			t.Errorf("Expected %v, got %v", exp, got)
		}

		assertNumEntries(t, bucket, "*", 0)
		assertError(t, w.Discard())
	})

	t.Run("globs", func(t *testing.T) {
		writeTestData(t, bucket, "path/a/first.txt")
		writeTestData(t, bucket, "path/b/second.txt")
		writeTestData(t, bucket, "path/a/third.json")

		assertNumEntries(t, bucket, "*", 0)
		assertNumEntries(t, bucket, "", 0)
		assertNumEntries(t, bucket, "path/*", 0)
		assertNumEntries(t, bucket, "path/*/*", 3)
		assertNumEntries(t, bucket, "*/*/*", 3)
		assertNumEntries(t, bucket, "*/a/*", 2)
		assertNumEntries(t, bucket, "*/b/*", 1)
		assertNumEntries(t, bucket, "path/*/*.txt", 2)
		assertNumEntries(t, bucket, "path/*/[ft]*", 2)
		assertNumEntries(t, bucket, "path/*/[ft]*.json", 1)
		assertNumEntries(t, bucket, "**", 3)

		assertNoError(t, bucket.Remove(ctx, "path/a/first.txt"))
		assertNoError(t, bucket.Remove(ctx, "path/b/second.txt"))
		assertNoError(t, bucket.Remove(ctx, "path/a/third.json"))
	})

	t.Run("heads", func(t *testing.T) {
		writeTestData(t, bucket, "path/to/first.txt")
		if _, err := bucket.Head(ctx, "path/to/missing"); !errors.Is(err, bfs.ErrNotFound) {
			t.Fatalf("Expected %v, got %v", bfs.ErrNotFound, err)
		}

		info, err := bucket.Head(ctx, "path/to/first.txt")
		assertNoError(t, err)

		if exp, got := "path/to/first.txt", info.Name; exp != got {
			t.Errorf("Expected %v, got %v", exp, got)
		}
		if exp, got := int64(8), info.Size; exp != got {
			t.Errorf("Expected %v, got %v", exp, got)
		}
		if exp, got := time.Now(), info.ModTime; exp.Sub(got) > time.Minute {
			t.Errorf("Expected %v (±1m), got %v", exp, got)
		}

		if supports.Metadata {
			meta := bfs.Metadata{"Cust0m-Key": "VaLu3"}
			if exp, got := meta, info.Metadata; !reflect.DeepEqual(exp, got) {
				t.Errorf("Expected %v, got %v", exp, got)
			}
		}

		if supports.ContentType {
			if exp, got := "path/to/first.txt", info.ContentType; exp != got {
				t.Errorf("Expected %v, got %v", exp, got)
			}
		}

		assertNoError(t, bucket.Remove(ctx, "path/to/first.txt"))
	})

	t.Run("reads", func(t *testing.T) {
		writeTestData(t, bucket, "path/to/first.txt")

		if _, err := bucket.Open(ctx, "path/to/missing"); !errors.Is(err, bfs.ErrNotFound) {
			t.Fatalf("Expected %v, got %v", bfs.ErrNotFound, err)
		}

		obj, err := bucket.Open(ctx, "path/to/first.txt")
		assertNoError(t, err)

		data := make([]byte, 100)
		sz, err := obj.Read(data)
		assertNoError(t, err)

		if exp := 8; exp != sz {
			t.Errorf("Expected %v, got %v", exp, sz)
		}
		if exp, got := "TESTDATA", string(data[:sz]); exp != got {
			t.Errorf("Expected %v, got %v", exp, sz)
		}

		assertNoError(t, obj.Close())
		assertNoError(t, bucket.Remove(ctx, "path/to/first.txt"))
	})

	t.Run("removes", func(t *testing.T) {
		writeTestData(t, bucket, "path/to/first.txt")
		assertNumEntries(t, bucket, "**", 1)

		assertNoError(t, bucket.Remove(ctx, "path/to/first.txt"))
		assertNumEntries(t, bucket, "**", 0)

		assertNoError(t, bucket.Remove(ctx, "missing.txt"))
	})

	t.Run("copies", func(t *testing.T) {
		copier, ok := bucket.(interface {
			Copy(context.Context, string, string) error
		})
		if !ok {
			t.Skip("Copy is not supported")
		}

		writeTestData(t, bucket, "path/to/src.txt")
		assertNumEntries(t, bucket, "**", 1)
		assertNoError(t, copier.Copy(ctx, "path/to/src.txt", "path/to/dst.txt"))
		assertNumEntries(t, bucket, "**", 2)

		info, err := bucket.Head(ctx, "path/to/dst.txt")
		assertNoError(t, err)

		if exp, got := "path/to/dst.txt", info.Name; exp != got {
			t.Errorf("Expected %v, got %v", exp, got)
		}
		if exp, got := int64(8), info.Size; exp != got {
			t.Errorf("Expected %v, got %v", exp, got)
		}
		if exp, got := time.Now(), info.ModTime; exp.Sub(got) > time.Minute {
			t.Errorf("Expected %v (±1m), got %v", exp, got)
		}

		assertNoError(t, bucket.Remove(ctx, "path/to/src.txt"))
		assertNoError(t, bucket.Remove(ctx, "path/to/dst.txt"))
	})
}

func Readonly(t *testing.T, bucket bfs.Bucket) {
	t.Run("globs lots of files", func(t *testing.T) {
		const numReadonlySamples = 2121

		if exp, got := numReadonlySamples, len(glob(t, bucket, "*/*")); exp != got {
			t.Errorf("Expected %v, got %v", exp, got)
		}
		if exp, got := numReadonlySamples, len(glob(t, bucket, "**")); exp != got {
			t.Errorf("Expected %v, got %v", exp, got)
		}
	})
}

// ----------------------------------------------------------------------------

func assertNumEntries(t *testing.T, bucket bfs.Bucket, pat string, exp int) {
	t.Helper()

	if got := glob(t, bucket, pat); len(got) != exp {
		t.Errorf("Expected %d entries, got %d - %v", exp, len(got), got)
	}
}

func assertNoError(t *testing.T, err error) {
	t.Helper()

	if err != nil {
		t.Fatal("Unexpected error", err)
	}
}

func assertError(t *testing.T, err error) {
	t.Helper()

	if err == nil {
		t.Fatal("Expected error, but got none")
	}
}

func collect(iter bfs.Iterator) (entries []string) {
	for iter.Next() {
		entries = append(entries, iter.Name())
	}
	return entries
}

func glob(t *testing.T, bucket bfs.Bucket, pat string) []string {
	t.Helper()

	iter, err := bucket.Glob(context.Background(), pat)
	assertNoError(t, err)
	defer iter.Close()

	return collect(iter)
}

func writeTestData(t *testing.T, bucket bfs.Bucket, name string) {
	t.Helper()

	assertNoError(t, bfs.WriteObject(context.Background(), bucket, name, []byte("TESTDATA"), &bfs.WriteOptions{
		Metadata:    bfs.Metadata{"CuSt0m_key": "VaLu3"},
		ContentType: "text/plain",
	}))
}
