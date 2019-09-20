module github.com/bsm/bfs/bfsgs

go 1.12

require (
	cloud.google.com/go/storage v1.0.0
	github.com/bmatcuk/doublestar v1.1.5
	github.com/bsm/bfs v0.7.1
	github.com/google/go-cmp v0.3.1 // indirect
	github.com/onsi/ginkgo v1.8.0
	github.com/onsi/gomega v1.5.0
	go.opencensus.io v0.22.1 // indirect
	golang.org/x/exp v0.0.0-20190919035709-81c71964d733 // indirect
	golang.org/x/net v0.0.0-20190918130420-a8b05e9114ab // indirect
	golang.org/x/sys v0.0.0-20190919044723-0c1ff786ef13 // indirect
	golang.org/x/tools v0.0.0-20190920023704-c426260dee6e // indirect
	google.golang.org/api v0.10.0
	google.golang.org/appengine v1.6.2 // indirect
	google.golang.org/genproto v0.0.0-20190916214212-f660b8655731 // indirect
	google.golang.org/grpc v1.23.1 // indirect
)

replace github.com/bsm/bfs => ../
