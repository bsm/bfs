package bfs_test

import (
	"context"
	"io/ioutil"

	"github.com/bsm/bfs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Object", func() {
	var subject *bfs.Object
	var _ bfs.ObjectHandle = subject
	var ctx = context.Background()

	BeforeEach(func() {
		var err error
		subject, err = bfs.NewObject(ctx, "mem:///path/to/file.txt")
		Expect(err).NotTo(HaveOccurred())
	})

	It("should have a name", func() {
		Expect(subject.Name()).To(Equal("path/to/file.txt"))
	})

	It("should head/read/write", func() {
		_, err := subject.Head(ctx)
		Expect(err).To(Equal(bfs.ErrNotFound))

		_, err = subject.Open(ctx)
		Expect(err).To(Equal(bfs.ErrNotFound))

		w, err := subject.Create(ctx)
		Expect(err).ToNot(HaveOccurred())
		Expect(w.Write([]byte("TESTDATA"))).To(Equal(8))
		Expect(w.Close()).To(Succeed())

		i, err := subject.Head(ctx)
		Expect(err).ToNot(HaveOccurred())
		Expect(i.Size).To(Equal(int64(8)))
		Expect(i.Name).To(Equal("path/to/file.txt"))

		r, err := subject.Open(ctx)
		Expect(err).ToNot(HaveOccurred())
		Expect(ioutil.ReadAll(r)).To(Equal([]byte("TESTDATA")))
		Expect(r.Close()).To(Succeed())

		Expect(subject.Remove(ctx)).To(Succeed())
		_, err = subject.Head(ctx)
		Expect(err).To(Equal(bfs.ErrNotFound))
	})
})
