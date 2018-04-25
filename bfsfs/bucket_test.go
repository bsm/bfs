package bfsfs_test

import (
	"os"
	"path/filepath"

	"github.com/bsm/bfs/bfsfs"
	"github.com/bsm/bfs/testdata/lint"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Bucket", func() {
	var (
		subjectDir string
		data       lint.Data
	)

	BeforeEach(func() {
		subjectDir = filepath.Join(tempDir, "subject")
		Expect(os.MkdirAll(subjectDir, 0777)).To(Succeed())

		subject, err := bfsfs.New(subjectDir, tempDir)
		Expect(err).NotTo(HaveOccurred())

		readonly, err := bfsfs.New(readonlyDir, tempDir)
		Expect(err).NotTo(HaveOccurred())

		data.Subject = subject
		data.Readonly = readonly
	})

	AfterEach(func() {
		if subjectDir != "" {
			Expect(os.RemoveAll(subjectDir)).To(Succeed())
		}
	})

	Context("defaults", lint.Lint(&data))
})
