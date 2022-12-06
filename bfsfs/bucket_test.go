package bfsfs_test

import (
	"os"

	"github.com/bsm/bfs/bfsfs"
	"github.com/bsm/bfs/testdata/lint"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

var _ = Describe("Bucket", func() {
	var dir string
	var opts lint.Options

	BeforeEach(func() {
		var err error

		dir, err = os.MkdirTemp("", "bfsfs")
		Expect(err).NotTo(HaveOccurred())

		subject, err := bfsfs.New(dir, "")
		Expect(err).NotTo(HaveOccurred())

		opts = lint.Options{
			Subject: subject,
		}
	})

	AfterEach(func() {
		if dir != "" {
			Expect(os.RemoveAll(dir)).To(Succeed())
		}
	})

	Context("defaults", lint.Lint(&opts))
})
