module github.com/bsm/bfs/bfsgs

go 1.14

replace github.com/bsm/bfs => ../

require (
	cloud.google.com/go/storage v1.10.0
	github.com/bmatcuk/doublestar v1.3.1
	github.com/bsm/bfs v0.10.5-0.20200804104424-50dd9ff4f4d4
	github.com/onsi/ginkgo v1.14.0
	github.com/onsi/gomega v1.10.1
	google.golang.org/api v0.29.0
)
