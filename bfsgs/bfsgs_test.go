package bfsgs_test

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/bsm/bfs/bfsgs"
	"github.com/bsm/bfs/testdata/lint"
	"google.golang.org/api/iterator"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	testBucketName = "bsm-bfs-unittest"
)

var _ = Describe("Bucket", func() {
	var data = lint.Data{}

	BeforeEach(func() {
		if os.Getenv("BFSGS_TEST") == "" {
			Skip("test is disabled, enable via BFSGS_TEST")
		}

		ctx := context.Background()

		subject, err := bfsgs.New(ctx, testBucketName, &bfsgs.Config{
			Prefix: "x/" + strconv.FormatInt(time.Now().UnixNano(), 10),
		})
		Expect(err).NotTo(HaveOccurred())

		readonly, err := bfsgs.New(ctx, testBucketName, &bfsgs.Config{
			Prefix: "m/",
		})
		Expect(err).NotTo(HaveOccurred())

		data.Subject = subject
		data.Readonly = readonly
	})

	Context("defaults", lint.Lint(&data))
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
	iter := bucket.Objects(ctx, &storage.Query{
		Prefix: "x/",
	})

	for {
		attrs, err := iter.Next()
		if err == iterator.Done {
			break
		}
		Expect(err).NotTo(HaveOccurred())
		Expect(bucket.Object(attrs.Name).Delete(ctx)).To(Succeed())
	}
})
