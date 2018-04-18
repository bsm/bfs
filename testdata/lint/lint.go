package lint

import (
	"context"
	"time"

	"github.com/bsm/bfs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

type DefaultsData struct {
	Subject, Readonly bfs.Bucket
}

func Defaults(data *DefaultsData) func() {
	var subject, readonly bfs.Bucket
	var ctx = context.Background()

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

	drain := func(iter bfs.Iterator, err error) ([]string, error) {
		if err != nil {
			return nil, err
		}
		defer iter.Close()

		entries := make([]string, 0)
		for iter.Next() {
			entries = append(entries, iter.Name())
		}
		return entries, iter.Error()
	}

	return func() {

		BeforeEach(func() {
			subject = data.Subject
			readonly = data.Readonly
		})

		It("should write", func() {
			blank, err := subject.Create(ctx, "blank.txt")
			Expect(err).NotTo(HaveOccurred())
			defer blank.Close()

			Expect(drain(subject.Glob(ctx, "*"))).To(BeEmpty())
			Expect(blank.Close()).To(Succeed())
			Expect(drain(subject.Glob(ctx, "*"))).To(ConsistOf("blank.txt"))
		})

		It("should glob lots of files", func() {
			if readonly == nil {
				Skip("test is disabled")
			}

			entries, err := drain(readonly.Glob(ctx, "*/*"))
			Expect(err).NotTo(HaveOccurred())
			Expect(len(entries)).To(Equal(2121))
		})

		It("should glob", func() {
			Expect(writeObject("path/a/first.txt")).To(Succeed())
			Expect(writeObject("path/b/second.txt")).To(Succeed())
			Expect(writeObject("path/a/third.json")).To(Succeed())

			Expect(drain(subject.Glob(ctx, ""))).To(BeEmpty())
			Expect(drain(subject.Glob(ctx, "path/*"))).To(BeEmpty())
			Expect(drain(subject.Glob(ctx, "path/*/*"))).To(HaveLen(3))
			Expect(drain(subject.Glob(ctx, "*/*/*"))).To(HaveLen(3))
			Expect(drain(subject.Glob(ctx, "*/a/*"))).To(HaveLen(2))
			Expect(drain(subject.Glob(ctx, "*/b/*"))).To(HaveLen(1))
			Expect(drain(subject.Glob(ctx, "path/*/*.txt"))).To(HaveLen(2))
			Expect(drain(subject.Glob(ctx, "path/*/[ft]*"))).To(HaveLen(2))
			Expect(drain(subject.Glob(ctx, "path/*/[ft]*.json"))).To(HaveLen(1))
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

		It("should remove", func() {
			Expect(writeObject("path/to/first.txt")).To(Succeed())

			Expect(drain(subject.Glob(ctx, "*/*/*"))).To(HaveLen(1))
			Expect(subject.Remove(ctx, "path/to/first.txt")).To(Succeed())
			Expect(drain(subject.Glob(ctx, "*/*/*"))).To(BeEmpty())

			Expect(subject.Remove(ctx, "missing")).To(Succeed())
		})

	}
}
