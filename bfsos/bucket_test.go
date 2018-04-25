package bfsos_test

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/bsm/bfs/bfsos"
	"github.com/bsm/bfs/testdata/lint"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Bucket", func() {
	var (
		tmp  string
		data lint.Data
	)

	BeforeEach(func() {
		var err error
		tmp, err = ioutil.TempDir("", "bfsos")
		Expect(err).NotTo(HaveOccurred())

		subjectDir := filepath.Join(tmp, "subject")
		Expect(os.MkdirAll(subjectDir, 0777)).To(Succeed())

		readonlyDir := filepath.Join(tmp, "readonly")
		Expect(os.MkdirAll(readonlyDir, 0777)).To(Succeed())

		tmpDir := filepath.Join(tmp, "tmp")
		Expect(os.MkdirAll(tmpDir, 0777)).To(Succeed())

		populateReadonlyFiles(readonlyDir)

		subject, err := bfsos.New(subjectDir, tmpDir)
		Expect(err).NotTo(HaveOccurred())

		readonly, err := bfsos.New(readonlyDir, tmpDir)
		Expect(err).NotTo(HaveOccurred())

		data.Subject = subject
		data.Readonly = readonly
	})

	AfterEach(func() {
		if tmp != "" {
			Expect(os.RemoveAll(tmp)).To(Succeed())
		}
	})

	Context("defaults", lint.Lint(&data))
})
