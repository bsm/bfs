# BFS

[![GoDoc](https://godoc.org/github.com/bsm/bfs?status.svg)](https://godoc.org/github.com/bsm/bfs)
[![Test](https://github.com/bsm/bfs/actions/workflows/test.yml/badge.svg)](https://github.com/bsm/bfs/actions/workflows/test.yml)

Multi-adapter bucket-based file system abstraction.

## Documentation

For documentation and examples, please see https://godoc.org/github.com/bsm/bfs.

## Install

```
go get -u github.com/bsm/bfs
```

## Basic Usage

```go
package main

import (
	"fmt"

	"github.com/bsm/bfs"
)

func main() {
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
```
