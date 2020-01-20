module github.com/bsm/bfs/bfsgs

go 1.12

require (
	cloud.google.com/go v0.51.0 // indirect
	cloud.google.com/go/storage v1.5.0
	github.com/bmatcuk/doublestar v1.2.2
	github.com/bsm/bfs v0.8.1
	github.com/onsi/ginkgo v1.8.0
	github.com/onsi/gomega v1.5.0
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d // indirect
	golang.org/x/sys v0.0.0-20200113162924-86b910548bc1 // indirect
	golang.org/x/tools v0.0.0-20200114052453-d31a08c2edf2 // indirect
	google.golang.org/api v0.15.0
	google.golang.org/genproto v0.0.0-20200113173426-e1de0a7b01eb // indirect
)

replace github.com/bsm/bfs => ../
