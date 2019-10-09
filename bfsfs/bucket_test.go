package bfsfs_test

import (
	"io/ioutil"
	"os"

	"github.com/bsm/bfs/bfsfs"
	"github.com/bsm/bfs/testdata/lint"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Bucket", func() {
	var dir string
	var opts lint.Options

	BeforeEach(func() {
		var err error

		dir, err = ioutil.TempDir("", "bfsfs")
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
