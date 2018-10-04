package bfs_test

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsm/bfs"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// ------------------------------------------------------------------------

func init() {
	bfs.Register("mem", func(_ context.Context, u *url.URL) (bfs.Bucket, error) {
		return bfs.NewInMem(), nil
	})
}

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "bfs")
}
