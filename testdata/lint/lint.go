package lint

import (
	"context"
	"time"

	"github.com/bsm/bfs"
	"github.com/onsi/ginkgo"
	Ω "github.com/onsi/gomega"
)

const numReadonlySamples = 2121

// Data is passed to the lint test set
type Data struct {
	Subject, Readonly bfs.Bucket
}

// Lint implements a test set.
func Lint(data *Data) func() {
	var subject, readonly bfs.Bucket
	var ctx = context.Background()

	return func() {
		ginkgo.BeforeEach(func() {
			subject = data.Subject
			readonly = data.Readonly
		})

		ginkgo.It("should write", func() {
			blank, err := subject.Create(ctx, "blank.txt", nil)
			Ω.Expect(err).NotTo(Ω.HaveOccurred())
			defer blank.Close()

			Ω.Expect(subject.Glob(ctx, "*")).To(whenDrained(Ω.BeEmpty()))
			Ω.Expect(blank.Close()).To(Ω.Succeed())
			Ω.Expect(subject.Glob(ctx, "*")).To(whenDrained(Ω.ConsistOf("blank.txt")))
		})

		ginkgo.It("should glob lots of files", func() {
			if readonly == nil {
				ginkgo.Skip("test is disabled")
			}
			Ω.Expect(readonly.Glob(ctx, "*/*")).To(whenDrained(Ω.HaveLen(numReadonlySamples)))
			Ω.Expect(readonly.Glob(ctx, "**")).To(whenDrained(Ω.HaveLen(numReadonlySamples)))
		})

		ginkgo.It("should glob", func() {
			Ω.Expect(writeTestData(subject, "path/a/first.txt")).To(Ω.Succeed())
			Ω.Expect(writeTestData(subject, "path/b/second.txt")).To(Ω.Succeed())
			Ω.Expect(writeTestData(subject, "path/a/third.json")).To(Ω.Succeed())

			Ω.Expect(subject.Glob(ctx, "")).To(whenDrained(Ω.BeEmpty()))
			Ω.Expect(subject.Glob(ctx, "path/*")).To(whenDrained(Ω.BeEmpty()))
			Ω.Expect(subject.Glob(ctx, "path/*/*")).To(whenDrained(Ω.HaveLen(3)))
			Ω.Expect(subject.Glob(ctx, "*/*/*")).To(whenDrained(Ω.HaveLen(3)))
			Ω.Expect(subject.Glob(ctx, "*/a/*")).To(whenDrained(Ω.HaveLen(2)))
			Ω.Expect(subject.Glob(ctx, "*/b/*")).To(whenDrained(Ω.HaveLen(1)))
			Ω.Expect(subject.Glob(ctx, "path/*/*.txt")).To(whenDrained(Ω.HaveLen(2)))
			Ω.Expect(subject.Glob(ctx, "path/*/[ft]*")).To(whenDrained(Ω.HaveLen(2)))
			Ω.Expect(subject.Glob(ctx, "path/*/[ft]*.json")).To(whenDrained(Ω.HaveLen(1)))
			Ω.Expect(subject.Glob(ctx, "**")).To(whenDrained(Ω.HaveLen(3)))
		})

		ginkgo.It("should head", func() {
			Ω.Expect(writeTestData(subject, "path/to/first.txt")).To(Ω.Succeed())

			_, err := subject.Head(ctx, "path/to/missing")
			Ω.Expect(err).To(Ω.Equal(bfs.ErrNotFound))

			info, err := subject.Head(ctx, "path/to/first.txt")
			Ω.Expect(err).NotTo(Ω.HaveOccurred())
			Ω.Expect(info.Name).To(Ω.Equal("path/to/first.txt"))
			Ω.Expect(info.Size).To(Ω.Equal(int64(8)))
			Ω.Expect(info.ModTime).To(Ω.BeTemporally("~", time.Now(), 5*time.Second))
		})

		ginkgo.It("should read", func() {
			Ω.Expect(writeTestData(subject, "path/to/first.txt")).To(Ω.Succeed())

			_, err := subject.Open(ctx, "path/to/missing")
			Ω.Expect(err).To(Ω.Equal(bfs.ErrNotFound))

			obj, err := subject.Open(ctx, "path/to/first.txt")
			Ω.Expect(err).NotTo(Ω.HaveOccurred())

			data := make([]byte, 100)
			Ω.Expect(obj.Read(data)).To(Ω.Equal(8))
			Ω.Expect(string(data[:8])).To(Ω.Equal("TESTDATA"))
			Ω.Expect(obj.Close()).To(Ω.Succeed())
		})

		ginkgo.It("should remove", func() {
			Ω.Expect(writeTestData(subject, "path/to/first.txt")).To(Ω.Succeed())

			Ω.Expect(subject.Glob(ctx, "*/*/*")).To(whenDrained(Ω.HaveLen(1)))
			Ω.Expect(subject.Remove(ctx, "path/to/first.txt")).To(Ω.Succeed())
			Ω.Expect(subject.Glob(ctx, "*/*/*")).To(whenDrained(Ω.BeEmpty()))

			Ω.Expect(subject.Remove(ctx, "missing")).To(Ω.Succeed())
		})

		ginkgo.It("should copy", func() {
			copier, ok := subject.(interface {
				Copy(context.Context, string, string) error
			})
			if !ok {
				ginkgo.Skip("test is disabled")
			}

			Ω.Expect(writeTestData(subject, "path/to/src.txt")).To(Ω.Succeed())

			Ω.Expect(subject.Glob(ctx, "*/*/*")).To(whenDrained(Ω.HaveLen(1)))
			Ω.Expect(copier.Copy(ctx, "path/to/src.txt", "path/to/dst.txt")).To(Ω.Succeed())
			Ω.Expect(subject.Glob(ctx, "*/*/*")).To(whenDrained(Ω.HaveLen(2)))

			info, err := subject.Head(ctx, "path/to/dst.txt")
			Ω.Expect(err).NotTo(Ω.HaveOccurred())
			Ω.Expect(info.Name).To(Ω.Equal("path/to/dst.txt"))
			Ω.Expect(info.Size).To(Ω.Equal(int64(8)))
			Ω.Expect(info.ModTime).To(Ω.BeTemporally("~", time.Now(), 5*time.Second))
		})
	}
}

func writeTestData(bucket bfs.Bucket, name string) error {
	return bfs.WriteObject(context.Background(), bucket, name, []byte("TESTDATA"), nil)
}

func whenDrained(m Ω.OmegaMatcher) Ω.OmegaMatcher {
	return Ω.WithTransform(func(iter bfs.Iterator) []string {
		defer iter.Close()

		var entries []string
		for iter.Next() {
			entries = append(entries, iter.Name())
		}
		return entries
	}, m)
}
