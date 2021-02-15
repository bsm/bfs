module github.com/bsm/bfs/bfss3

go 1.14

replace github.com/bsm/bfs => ../

require (
	github.com/aws/aws-sdk-go-v2 v1.1.0
	github.com/aws/aws-sdk-go-v2/config v1.1.0
	github.com/aws/aws-sdk-go-v2/credentials v1.1.0
	github.com/aws/aws-sdk-go-v2/feature/s3/manager v1.0.1
	github.com/aws/aws-sdk-go-v2/service/s3 v1.1.0
	github.com/bmatcuk/doublestar v1.3.1
	github.com/bsm/bfs v0.10.5-0.20200804104424-50dd9ff4f4d4
	github.com/onsi/ginkgo v1.14.0
	github.com/onsi/gomega v1.10.1
)
