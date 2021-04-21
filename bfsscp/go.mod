module github.com/bsm/bfs/bfsscp

go 1.14

replace github.com/bsm/bfs => ../

require (
	github.com/bmatcuk/doublestar/v3 v3.0.0
	github.com/bsm/bfs v0.0.0-00010101000000-000000000000
	github.com/bsm/ginkgo v1.16.0
	github.com/bsm/gomega v1.11.0
	github.com/kr/fs v0.1.0
	github.com/pkg/sftp v1.13.0
	go.uber.org/multierr v1.6.0
	golang.org/x/crypto v0.0.0-20210415154028-4f45737414dc
)
