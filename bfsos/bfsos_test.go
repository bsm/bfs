package bfsos_test

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

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "bfs/bfsos")
}

// ------------------------------------------------------------------------

func populateReadonlyFiles(dir string) {
	subdir := filepath.Join(dir, "subdir")
	Expect(os.MkdirAll(subdir, 0777)).To(Succeed())

	for i := 1; i <= lint.NumReadonlySamples; i++ {
		name := filepath.Join(subdir, fmt.Sprintf("%06d.txt", i))
		Expect(ioutil.WriteFile(name, []byte(name), 0777)).To(Succeed())
	}
}
