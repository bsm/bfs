package bfs_test

import (
	"context"
	"time"

	"github.com/bsm/bfs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("InMem", func() {
	var subject *bfs.InMem
	var _ bfs.Bucket = subject
	var ctx = context.Background()

	BeforeEach(func() {
		subject = bfs.NewInMem()
	})

	writeObject := func(name string) error {
		o, err := subject.Create(ctx, name)
		if err != nil {
			return err
		}
		defer o.Close()

		if _, err := o.Write([]byte("TESTDATA")); err != nil {
			return err
		}
		return o.Close()
	}

	It("should write", func() {
		blank, err := subject.Create(ctx, "blank.txt")
		Expect(err).NotTo(HaveOccurred())
		defer blank.Close()

		Expect(subject.Glob(ctx, "*")).To(BeEmpty())
		Expect(blank.Close()).To(Succeed())
		Expect(subject.Glob(ctx, "*")).To(ConsistOf("blank.txt"))
	})

	It("should glob", func() {
		Expect(writeObject("path/a/first.txt")).To(Succeed())
		Expect(writeObject("path/b/second.txt")).To(Succeed())
		Expect(writeObject("path/a/third.json")).To(Succeed())

		Expect(subject.Glob(ctx, "path/*")).To(BeEmpty())
		Expect(subject.Glob(ctx, "path/*/*")).To(HaveLen(3))
		Expect(subject.Glob(ctx, "*/*/*")).To(HaveLen(3))
		Expect(subject.Glob(ctx, "*/a/*")).To(HaveLen(2))
		Expect(subject.Glob(ctx, "*/b/*")).To(HaveLen(1))
		Expect(subject.Glob(ctx, "path/*/*.txt")).To(HaveLen(2))
		Expect(subject.Glob(ctx, "path/*/[ft]*")).To(HaveLen(2))
		Expect(subject.Glob(ctx, "path/*/[ft]*.json")).To(HaveLen(1))
	})

	It("should head", func() {
		Expect(writeObject("path/to/first.txt")).To(Succeed())

		_, err := subject.Head(ctx, "path/to/missing")
		Expect(err).To(Equal(bfs.ErrNotFound))

		info, err := subject.Head(ctx, "path/to/first.txt")
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Name).To(Equal("path/to/first.txt"))
		Expect(info.Size).To(Equal(int64(8)))
		Expect(info.ModTime).To(BeTemporally("~", time.Now(), time.Second))
	})

	It("should read", func() {
		Expect(writeObject("path/to/first.txt")).To(Succeed())

		_, err := subject.Open(ctx, "path/to/missing")
		Expect(err).To(Equal(bfs.ErrNotFound))

		obj, err := subject.Open(ctx, "path/to/first.txt")
		Expect(err).NotTo(HaveOccurred())

		data := make([]byte, 100)
		Expect(obj.Read(data)).To(Equal(8))
		Expect(string(data[:8])).To(Equal("TESTDATA"))
	})

})
