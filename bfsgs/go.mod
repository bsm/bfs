module github.com/bsm/bfs/bfsgs

go 1.12

require (
	cloud.google.com/go v0.44.3
	github.com/bmatcuk/doublestar v1.1.5
	github.com/bsm/bfs v0.7.0
	github.com/google/go-cmp v0.3.1 // indirect
	github.com/hashicorp/golang-lru v0.5.3 // indirect
	github.com/onsi/ginkgo v1.8.0
	github.com/onsi/gomega v1.5.0
	google.golang.org/api v0.8.0
	google.golang.org/grpc v1.23.0 // indirect
)

replace github.com/bsm/bfs => ../
