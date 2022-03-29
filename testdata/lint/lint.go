package lint

import (
	"context"
	"errors"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/ginkgo/v2"
	Ω "github.com/bsm/gomega"
)

const numReadonlySamples = 2121

// Options are passed to the lint test set
type Options struct {
	Subject, Readonly bfs.Bucket

	Metadata    bool
	ContentType bool
}

// Lint implements a test set.
func Lint(opts *Options) func() {
	var subject, readonly bfs.Bucket
	var ctx = context.Background()

	return func() {
		ginkgo.BeforeEach(func() {
			subject = opts.Subject
			readonly = opts.Readonly
		})

		ginkgo.It("writes", func() {
			blank, err := subject.Create(ctx, "blank.txt", nil)
			Ω.Expect(err).NotTo(Ω.HaveOccurred())
			defer blank.Discard()

			Ω.Expect(subject.Glob(ctx, "*")).To(whenDrained(Ω.BeEmpty()))
			Ω.Expect(blank.Commit()).To(Ω.Succeed())
			Ω.Expect(subject.Glob(ctx, "*")).To(whenDrained(Ω.ConsistOf("blank.txt")))
			Ω.Expect(blank.Discard()).NotTo(Ω.Succeed())
		})

		ginkgo.It("aborts write if discarded", func() {
			blank, err := subject.Create(ctx, "blank.txt", nil)
			Ω.Expect(err).NotTo(Ω.HaveOccurred())
			defer blank.Discard()

			Ω.Expect(subject.Glob(ctx, "*")).To(whenDrained(Ω.BeEmpty()))
			Ω.Expect(blank.Discard()).To(Ω.Succeed())
			Ω.Expect(blank.Commit()).NotTo(Ω.Succeed())
			Ω.Expect(subject.Glob(ctx, "*")).To(whenDrained(Ω.BeEmpty()))
		})

		ginkgo.It("aborts write if context is cancelled", func() {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()

			blank, err := subject.Create(ctx, "blank.txt", nil)
			Ω.Expect(err).NotTo(Ω.HaveOccurred())
			defer blank.Discard()

			Ω.Expect(subject.Glob(ctx, "*")).To(whenDrained(Ω.BeEmpty()))
			cancel()

			commitErr := blank.Commit()
			Ω.Expect(errors.Is(commitErr, context.Canceled)).To(Ω.BeTrue())

			Ω.Expect(subject.Glob(ctx, "*")).To(whenDrained(Ω.BeEmpty()))
			Ω.Expect(blank.Discard()).NotTo(Ω.Succeed())
		})

		ginkgo.It("globs many files", func() {
			if readonly == nil {
				ginkgo.Skip("test is disabled")
			}
			Ω.Expect(readonly.Glob(ctx, "*/*")).To(whenDrained(Ω.HaveLen(numReadonlySamples)))
			Ω.Expect(readonly.Glob(ctx, "**")).To(whenDrained(Ω.HaveLen(numReadonlySamples)))
		})

		ginkgo.It("globs", func() {
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

		ginkgo.It("heads", func() {
			Ω.Expect(writeTestData(subject, "path/to/first.txt")).To(Ω.Succeed())

			_, err := subject.Head(ctx, "path/to/missing")
			Ω.Expect(err).To(Ω.Equal(bfs.ErrNotFound))

			info, err := subject.Head(ctx, "path/to/first.txt")
			Ω.Expect(err).NotTo(Ω.HaveOccurred())
			Ω.Expect(info.Name).To(Ω.Equal("path/to/first.txt"))
			Ω.Expect(info.Size).To(Ω.Equal(int64(8)))
			Ω.Expect(info.ModTime).To(Ω.BeTemporally("~", time.Now(), time.Minute))

			if opts.Metadata {
				Ω.Expect(info.Metadata).To(Ω.Equal(bfs.Metadata{
					"Cust0m-Key": "VaLu3",
				}))
			}
			if opts.ContentType {
				Ω.Expect(info.ContentType).To(Ω.Equal("text/plain"))
			}
		})

		ginkgo.It("reads", func() {
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

		ginkgo.It("removes", func() {
			Ω.Expect(writeTestData(subject, "path/to/first.txt")).To(Ω.Succeed())

			Ω.Expect(subject.Glob(ctx, "*/*/*")).To(whenDrained(Ω.HaveLen(1)))
			Ω.Expect(subject.Remove(ctx, "path/to/first.txt")).To(Ω.Succeed())
			Ω.Expect(subject.Glob(ctx, "*/*/*")).To(whenDrained(Ω.BeEmpty()))

			Ω.Expect(subject.Remove(ctx, "missing")).To(Ω.Succeed())
		})

		ginkgo.It("copies", func() {
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
	return bfs.WriteObject(context.Background(), bucket, name, []byte("TESTDATA"), &bfs.WriteOptions{
		Metadata:    bfs.Metadata{"CuSt0m_key": "VaLu3"},
		ContentType: "text/plain",
	})
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
