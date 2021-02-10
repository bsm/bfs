package bfss3_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/bfs/bfss3"
	"github.com/bsm/bfs/testdata/lint"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const bucketName = "bsm-bfs-unittest"

var awsConfig aws.Config

func init() {
	c, err := config.LoadDefaultConfig(context.Background(), config.WithRegion("us-east-1"))
	if err != nil {
		panic(err)
	}
	awsConfig = c
}

var _ = Describe("Bucket", func() {
	var opts lint.Options

	ctx := context.Background()

	BeforeEach(func() {
		prefix := "x/" + strconv.FormatInt(time.Now().UnixNano(), 10)
		subject, err := bfss3.New(ctx, bucketName, &bfss3.Config{Prefix: prefix, AWS: &awsConfig})
		Expect(err).NotTo(HaveOccurred())

		readonly, err := bfss3.New(ctx, bucketName, &bfss3.Config{Prefix: "m/", AWS: &awsConfig})
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
		fmt.Printf("ERR %#v\n", err)
		t.Skipf("skipping test, no sandbox access: %v", err)
		return
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "bfs/bfss3")
}

func sandboxCheck() error {
	ctx := context.Background()
	b, err := bfss3.New(ctx, bucketName, &bfss3.Config{AWS: &awsConfig})
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
	b, err := bfss3.New(ctx, bucketName, &bfss3.Config{Prefix: "x/", AWS: &awsConfig})
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
