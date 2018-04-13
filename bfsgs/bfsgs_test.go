package bfsgs_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/bsm/bfs"
	"github.com/bsm/bfs/bfsgs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"google.golang.org/api/iterator"
)

const (
	testBucketName = "bsm-bfs-unittest"
)

var _ = Describe("Bucket", func() {
	var subject bfs.Bucket
	var prefix string
	var ctx = context.Background()

	BeforeEach(func() {
		if os.Getenv("BFSGS_TEST") == "" {
			Skip("test is disabled, enable via BFSGS_TEST")
		}

		prefix = strconv.FormatInt(time.Now().UnixNano(), 10)

		var err error
		subject, err = bfsgs.New(ctx, testBucketName, &bfsgs.Config{
			Prefix: prefix,
		})
		Expect(err).NotTo(HaveOccurred())
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

	It("should remove", func() {
		Expect(writeObject("path/to/first.txt")).To(Succeed())

		Expect(subject.Glob(ctx, "*/*/*")).To(HaveLen(1))
		Expect(subject.Remove(ctx, "path/to/first.txt")).To(Succeed())
		Expect(subject.Glob(ctx, "*/*/*")).To(BeEmpty())

		Expect(subject.Remove(ctx, "missing")).To(Succeed())
	})

})

// ------------------------------------------------------------------------

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "bfs/bfsgs")
}

var _ = AfterSuite(func() {
	if os.Getenv("BFSGS_TEST") == "" {
		return
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	Expect(err).NotTo(HaveOccurred())

	bucket := client.Bucket(testBucketName)
	iter := bucket.Objects(ctx, nil)

	for {
		attrs, err := iter.Next()
		if err == iterator.Done {
			break
		}
		Expect(err).NotTo(HaveOccurred())
		Expect(bucket.Object(attrs.Name).Delete(ctx)).To(Succeed())
	}
})
