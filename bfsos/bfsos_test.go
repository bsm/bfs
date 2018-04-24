package bfsos_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/bsm/bfs/bfsos"
	"github.com/bsm/bfs/testdata/lint"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	tmpRoot = "testdata/tmp"
)

var _ = Describe("Bucket", func() {
	var tmpDir string
	var data = lint.Data{}

	BeforeEach(func() {
		if os.Getenv("BFSOS_TEST") == "" {
			Skip("test is disabled, enable via BFSOS_TEST")
		}

		Expect(os.MkdirAll(tmpRoot, 0755)).To(Succeed())

		var err error
		tmpDir, err = ioutil.TempDir(tmpRoot, "test")
		Expect(err).NotTo(HaveOccurred())

		subject, err := bfsos.New(tmpDir)
		Expect(err).NotTo(HaveOccurred())

		data.Subject = subject
	})

	AfterEach(func() {
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	Context("defaults", lint.Lint(&data))
})

// ------------------------------------------------------------------------

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "bfs/bfsos")
}

var _ = AfterSuite(func() {
	if os.Getenv("BFSOS_TEST") == "" {
		return
	}

	Expect(os.RemoveAll(tmpRoot))
})
