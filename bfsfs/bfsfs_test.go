package bfsfs_test

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/bsm/bfs/testdata/lint"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var (
	tempDir     string
	readonlyDir string
)

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "bfs/bfsfs")
}

var _ = BeforeSuite(func() {
	var err error
	tempDir, err = ioutil.TempDir("", "bfsfs")
	Expect(err).NotTo(HaveOccurred())

	readonlyDir = filepath.Join(tempDir, "readonly")
	Expect(os.MkdirAll(readonlyDir, 0777)).To(Succeed())

	subdir := filepath.Join(readonlyDir, "subdir")
	Expect(os.MkdirAll(subdir, 0777)).To(Succeed())

	for i := 1; i <= lint.NumReadonlySamples; i++ {
		name := filepath.Join(subdir, fmt.Sprintf("%06d.txt", i))
		Expect(ioutil.WriteFile(name, []byte(name), 0777)).To(Succeed())
	}

})

var _ = AfterSuite(func() {
	if tempDir != "" {
		Expect(os.RemoveAll(tempDir)).To(Succeed())
	}
})
