package bfsfs_test

import (
	"testing"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "bfs/bfsfs")
}
