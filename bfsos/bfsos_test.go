package bfsos_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/bsm/bfs/bfsos"
	"github.com/bsm/bfs/testdata/lint"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	tmpRoot     = "testdata/tmp"
	readonlyDir = "testdata/tmp/readonly"
)

var _ = Describe("Bucket", func() {
	var rootDir, tmpDir string
	var data = lint.Data{}

	BeforeEach(func() {
		if os.Getenv("BFSOS_TEST") == "" {
			Skip("test is disabled, enable via BFSOS_TEST")
		}

		Expect(os.MkdirAll(tmpRoot, 0755)).To(Succeed())

		var err error

		rootDir, err = ioutil.TempDir(tmpRoot, "test")
		Expect(err).NotTo(HaveOccurred())

		tmpDir, err = ioutil.TempDir(tmpRoot, "tmp")
		Expect(err).NotTo(HaveOccurred())

		subject, err := bfsos.New(rootDir, tmpDir)
		Expect(err).NotTo(HaveOccurred())

		readonly, err := bfsos.New(readonlyDir, tmpDir)
		Expect(err).NotTo(HaveOccurred())

		data.Subject = subject
		data.Readonly = readonly
	})

	AfterEach(func() {
		Expect(os.RemoveAll(rootDir)).To(Succeed())
		Expect(os.RemoveAll(tmpDir)).To(Succeed())
	})

	Context("defaults", lint.Lint(&data))
})

// ------------------------------------------------------------------------

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "bfs/bfsos")
}

var _ = BeforeSuite(func() {
	if os.Getenv("BFSOS_TEST") == "" {
		return
	}

	Expect(os.MkdirAll(filepath.Join(readonlyDir, "readonly"), 0755))
	for i := 1; i <= lint.NumReadonlySamples; i++ {
		name := filepath.Join(readonlyDir, "readonly", fmt.Sprintf("%06d.txt", i))
		Expect(ioutil.WriteFile(name, []byte(name), 0644)).To(Succeed())
	}
})

var _ = AfterSuite(func() {
	if os.Getenv("BFSOS_TEST") == "" {
		return
	}

	Expect(os.RemoveAll(tmpRoot))
})
