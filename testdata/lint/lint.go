package lint

import (
	"context"
	"time"

	"github.com/bsm/bfs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const NumReadonlySamples = 2121

type Data struct {
	Subject, Readonly bfs.Bucket
}

func Lint(data *Data) func() {
	var subject, readonly bfs.Bucket
	var ctx = context.Background()

	return func() {
		BeforeEach(func() {
			subject = data.Subject
			readonly = data.Readonly
		})

		It("should write", func() {
			blank, err := subject.Create(ctx, "blank.txt", nil)
			Expect(err).NotTo(HaveOccurred())
			defer blank.Close()

			Expect(subject.Glob(ctx, "*")).To(whenDrained(BeEmpty()))
			Expect(blank.Close()).To(Succeed())
			Expect(subject.Glob(ctx, "*")).To(whenDrained(ConsistOf("blank.txt")))
		})

		It("should glob lots of files", func() {
			if readonly == nil {
				Skip("test is disabled")
			}
			Expect(readonly.Glob(ctx, "*/*")).To(whenDrained(HaveLen(NumReadonlySamples)))
			Expect(readonly.Glob(ctx, "**")).To(whenDrained(HaveLen(NumReadonlySamples)))
		})

		It("should glob", func() {
			Expect(writeTestData(subject, "path/a/first.txt")).To(Succeed())
			Expect(writeTestData(subject, "path/b/second.txt")).To(Succeed())
			Expect(writeTestData(subject, "path/a/third.json")).To(Succeed())

			Expect(subject.Glob(ctx, "")).To(whenDrained(BeEmpty()))
			Expect(subject.Glob(ctx, "path/*")).To(whenDrained(BeEmpty()))
			Expect(subject.Glob(ctx, "path/*/*")).To(whenDrained(HaveLen(3)))
			Expect(subject.Glob(ctx, "*/*/*")).To(whenDrained(HaveLen(3)))
			Expect(subject.Glob(ctx, "*/a/*")).To(whenDrained(HaveLen(2)))
			Expect(subject.Glob(ctx, "*/b/*")).To(whenDrained(HaveLen(1)))
			Expect(subject.Glob(ctx, "path/*/*.txt")).To(whenDrained(HaveLen(2)))
			Expect(subject.Glob(ctx, "path/*/[ft]*")).To(whenDrained(HaveLen(2)))
			Expect(subject.Glob(ctx, "path/*/[ft]*.json")).To(whenDrained(HaveLen(1)))
			Expect(subject.Glob(ctx, "**")).To(whenDrained(HaveLen(3)))
		})

		It("should head", func() {
			Expect(writeTestData(subject, "path/to/first.txt")).To(Succeed())

			_, err := subject.Head(ctx, "path/to/missing")
			Expect(err).To(Equal(bfs.ErrNotFound))

			info, err := subject.Head(ctx, "path/to/first.txt")
			Expect(err).NotTo(HaveOccurred())
			Expect(info.Name).To(Equal("path/to/first.txt"))
			Expect(info.Size).To(Equal(int64(8)))
			Expect(info.ModTime).To(BeTemporally("~", time.Now(), time.Second))
		})

		It("should read", func() {
			Expect(writeTestData(subject, "path/to/first.txt")).To(Succeed())

			_, err := subject.Open(ctx, "path/to/missing")
			Expect(err).To(Equal(bfs.ErrNotFound))

			obj, err := subject.Open(ctx, "path/to/first.txt")
			Expect(err).NotTo(HaveOccurred())

			data := make([]byte, 100)
			Expect(obj.Read(data)).To(Equal(8))
			Expect(string(data[:8])).To(Equal("TESTDATA"))
			Expect(obj.Close()).To(Succeed())
		})

		It("should remove", func() {
			Expect(writeTestData(subject, "path/to/first.txt")).To(Succeed())

			Expect(subject.Glob(ctx, "*/*/*")).To(whenDrained(HaveLen(1)))
			Expect(subject.Remove(ctx, "path/to/first.txt")).To(Succeed())
			Expect(subject.Glob(ctx, "*/*/*")).To(whenDrained(BeEmpty()))

			Expect(subject.Remove(ctx, "missing")).To(Succeed())
		})

		It("should copy", func() {
			copier, ok := subject.(interface {
				Copy(context.Context, string, string) error
			})
			if !ok {
				Skip("test is disabled")
			}

			Expect(writeTestData(subject, "path/to/src.txt")).To(Succeed())

			Expect(subject.Glob(ctx, "*/*/*")).To(whenDrained(HaveLen(1)))
			Expect(copier.Copy(ctx, "path/to/src.txt", "path/to/dst.txt")).To(Succeed())
			Expect(subject.Glob(ctx, "*/*/*")).To(whenDrained(HaveLen(2)))

			info, err := subject.Head(ctx, "path/to/dst.txt")
			Expect(err).NotTo(HaveOccurred())
			Expect(info.Name).To(Equal("path/to/dst.txt"))
			Expect(info.Size).To(Equal(int64(8)))
			Expect(info.ModTime).To(BeTemporally("~", time.Now(), time.Second))
		})

	}
}

func writeTestData(bucket bfs.Bucket, name string) error {
	return bfs.WriteObject(bucket, context.Background(), name, []byte("TESTDATA"), nil)
}

func whenDrained(m OmegaMatcher) OmegaMatcher {
	return WithTransform(func(iter bfs.Iterator) []string {
		defer iter.Close()

		var entries []string
		for iter.Next() {
			entries = append(entries, iter.Name())
		}
		return entries
	}, m)
}
