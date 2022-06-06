package bfs_test

import (
	"github.com/bsm/bfs"
	"github.com/bsm/bfs/testdata/lint"
	. "github.com/bsm/ginkgo/v2"
)

var _ = Describe("InMem", func() {
	var subject *bfs.InMem
	var _ bfs.Bucket = subject
	var opts lint.Options

	BeforeEach(func() {
		subject = bfs.NewInMem()
		opts = lint.Options{
			Subject:  subject,
			Metadata: true,
		}
	})

	Context("defaults", lint.Lint(&opts))
})
