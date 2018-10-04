package internal_test

import (
	"testing"

	"github.com/bsm/bfs/internal"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"
)

var _ = DescribeTable("WithinNamespace",
	func(name, expected string) {
		Expect(internal.WithinNamespace("/my/root", name)).To(Equal(expected))
	},
	Entry("blank", "", "/my/root"),
	Entry("relative", "file/name.txt", "/my/root/file/name.txt"),
	Entry("absolute", "/file/name.txt", "/my/root/file/name.txt"),
	Entry("dirty", "//file//name.txt", "/my/root/file/name.txt"),
	Entry("with parent references", "/file/../name.txt", "/my/root/name.txt"),
	Entry("escape attempts", "../file/secret.txt", "/my/root/file/secret.txt"),
	Entry("clever escape attempts", "/file/../../../../secret.txt", "/my/root/secret.txt"),
)

// ------------------------------------------------------------------------

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "bfs/internal")
}
