package bfsgs_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/bsm/bfs/bfsgs"
	"github.com/bsm/bfs/testdata/lint"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const bucketName = "bsm-bfs-unittest"

var _ = Describe("Bucket", func() {
	var data = lint.Data{}

	BeforeEach(func() {
		if skipTest {
			Skip("test is disabled, could not connect to test bucket")
		}

		ctx := context.Background()

		subject, err := bfsgs.New(ctx, bucketName, &bfsgs.Config{
			Prefix: "x/" + strconv.FormatInt(time.Now().UnixNano(), 10),
		})
		Expect(err).NotTo(HaveOccurred())

		readonly, err := bfsgs.New(ctx, bucketName, &bfsgs.Config{
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

var skipTest bool

func init() {
	ctx := context.Background()
	b, err := bfsgs.New(ctx, bucketName, nil)
	if err != nil {
		skipTest = true
		return
	}
	defer b.Close()

	if _, err := b.Glob(ctx, "*"); err != nil {
		skipTest = true
		return
	}
}

var _ = AfterSuite(func() {
	if skipTest {
		return
	}

	ctx := context.Background()
	b, err := bfsgs.New(ctx, bucketName, &bfsgs.Config{Prefix: "x/"})
	Expect(err).NotTo(HaveOccurred())
	defer b.Close()

	it, err := b.Glob(ctx, "**")
	Expect(err).NotTo(HaveOccurred())
	defer it.Close()

	for it.Next() {
		Expect(b.Remove(ctx, it.Name())).To(Succeed())
	}
	Expect(it.Error()).NotTo(HaveOccurred())
})
