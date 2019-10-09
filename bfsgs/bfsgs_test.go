package bfsgs_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/bfs/bfsgs"
	"github.com/bsm/bfs/testdata/lint"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const bucketName = "bsm-bfs-unittest"

var _ = Describe("Bucket", func() {
	var opts lint.Options

	BeforeEach(func() {
		ctx := context.Background()

		prefix := "x/" + strconv.FormatInt(time.Now().UnixNano(), 10)
		subject, err := bfsgs.New(ctx, bucketName, &bfsgs.Config{Prefix: prefix})
		Expect(err).NotTo(HaveOccurred())

		readonly, err := bfsgs.New(ctx, bucketName, &bfsgs.Config{Prefix: "m/"})
		Expect(err).NotTo(HaveOccurred())

		opts = lint.Options{
			Subject:  subject,
			Readonly: readonly,

			Metadata:    true,
			ContentType: true,
		}
	})

	Context("defaults", lint.Lint(&opts))
})

// ------------------------------------------------------------------------

func TestSuite(t *testing.T) {
	if err := sandboxCheck(); err != nil {
		t.Skipf("skipping test, no sandbox access: %v", err)
		return
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "bfs/bfsgs")
}

func sandboxCheck() error {
	ctx := context.Background()
	b, err := bfsgs.New(ctx, bucketName, nil)
	if err != nil {
		return err
	}
	defer b.Close()

	if _, err := b.Head(ctx, "____"); err != bfs.ErrNotFound {
		return err
	}
	return nil
}

var _ = AfterSuite(func() {
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
