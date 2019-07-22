package bfs_test

import (
	"context"

	"github.com/bsm/bfs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Helpers", func() {
	var bucket *bfs.InMem
	var ctx = context.Background()

	BeforeEach(func() {
		bucket = bfs.NewInMem()
	})

	It("should write objects", func() {
		err := bfs.WriteObject(ctx, bucket, "path/to/file", []byte("testdata"), nil)
		Expect(err).NotTo(HaveOccurred())

		Expect(bucket.ObjectSizes()).
			To(HaveKeyWithValue("path/to/file", int64(8)))
	})

	It("should copy objects", func() {
		err := bfs.WriteObject(ctx, bucket, "src.txt", []byte("testdata"), nil)
		Expect(err).NotTo(HaveOccurred())

		err = bfs.CopyObject(ctx, bucket, "src.txt", "dst.txt", nil)
		Expect(err).NotTo(HaveOccurred())

		Expect(bucket.ObjectSizes()).
			To(HaveKeyWithValue("src.txt", int64(8)))
		Expect(bucket.ObjectSizes()).
			To(HaveKeyWithValue("dst.txt", int64(8)))
	})
})
