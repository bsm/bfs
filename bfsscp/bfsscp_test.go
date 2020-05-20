package bfsscp_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/bsm/bfs"
	"github.com/bsm/bfs/bfsscp"
	"github.com/bsm/bfs/testdata/lint"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	serverAddr = "127.0.0.1:7022"
)

var _ = Describe("Bucket", func() {
	var opts lint.Options

	BeforeEach(func() {
		prefix := "x/" + strconv.FormatInt(time.Now().UnixNano(), 10)
		subject, err := bfsscp.New(serverAddr, &bfsscp.Config{Prefix: prefix, Username: "root", Password: "root"})
		Expect(err).NotTo(HaveOccurred())

		opts = lint.Options{
			Subject: subject,
		}
	})

	It("should register scp and ssh schemes", func() {
		subject, err := bfs.Connect(context.Background(), "scp://root:root@127.0.0.1:7022/prefix?tmpdir=test")
		Expect(err).NotTo(HaveOccurred())
		Expect(subject.Close()).NotTo(HaveOccurred())

		subject, err = bfs.Connect(context.Background(), "ssh://root:root@127.0.0.1:7022/prefix?tmpdir=test")
		Expect(err).NotTo(HaveOccurred())
		Expect(subject.Close()).NotTo(HaveOccurred())
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
	RunSpecs(t, "bfs/bfsscp")
}

func sandboxCheck() error {
	ctx := context.Background()
	b, err := bfsscp.New(serverAddr, &bfsscp.Config{Username: "root", Password: "root"})
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
	b, err := bfsscp.New(serverAddr, &bfsscp.Config{Prefix: "x/", Username: "root", Password: "root"})
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
