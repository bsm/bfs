package bfs_test

import (
	"context"
	"fmt"
	"io/ioutil"

	"github.com/bsm/bfs"
)

func ExampleInMem() {
	ctx := context.Background()
	bucket := bfs.NewInMem()

	// Write object
	o1, err := bucket.Create(ctx, "nested/file.txt", nil)
	if err != nil {
		panic(err)
	}
	defer o1.Discard()

	if _, err := o1.Write([]byte("TESTDATA")); err != nil {
		panic(err)
	}
	if err := o1.Commit(); err != nil {
		panic(err)
	}

	// Glob entries
	entries, err := bucket.Glob(ctx, "nested/**")
	if err != nil {
		panic(err)
	}
	fmt.Println("ENTRIES:", entries)

	// Read object
	o2, err := bucket.Open(ctx, "nested/file.txt")
	if err != nil {
		panic(err)
	}
	defer o2.Close()

	data, err := ioutil.ReadAll(o2)
	if err != nil {
		panic(err)
	}
	fmt.Println("DATA:", string(data))

	// Head object
	info, err := bucket.Head(ctx, "nested/file.txt")
	if err != nil {
		panic(err)
	}
	fmt.Printf("INFO: name=%q size=%d\n", info.Name, info.Size)

	// Delete object
	if err := bucket.Remove(ctx, "nested/file.txt"); err != nil {
		panic(err)
	}
}
