package bfs_test

import (
	"github.com/bsm/bfs"
	"github.com/bsm/bfs/testdata/lint"
	. "github.com/onsi/ginkgo"
)

var _ = Describe("InMem", func() {
	var subject *bfs.InMem
	var _ bfs.Bucket = subject
	var data = lint.DefaultsData{}

	BeforeEach(func() {
		subject = bfs.NewInMem()
		data.Subject = subject
	})

	Context("defaults", lint.Defaults(&data))
})
