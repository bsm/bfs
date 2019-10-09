package bfsftp_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/bfs/bfsftp"
	"github.com/bsm/bfs/testdata/lint"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	serverAddr = "127.0.0.1:7021"
)

var _ = Describe("Bucket", func() {
	var opts lint.Options

	BeforeEach(func() {
		prefix := "x/" + strconv.FormatInt(time.Now().UnixNano(), 10)
		subject, err := bfsftp.New(serverAddr, &bfsftp.Config{Prefix: prefix, Username: "ftpuser", Password: "ftppass"})
		Expect(err).NotTo(HaveOccurred())

		opts = lint.Options{
			Subject: subject,
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
	RunSpecs(t, "bfs/bfsftp")
}

func sandboxCheck() error {
	ctx := context.Background()
	b, err := bfsftp.New(serverAddr, &bfsftp.Config{Username: "ftpuser", Password: "ftppass"})
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
	b, err := bfsftp.New(serverAddr, &bfsftp.Config{Prefix: "x/", Username: "ftpuser", Password: "ftppass"})
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
