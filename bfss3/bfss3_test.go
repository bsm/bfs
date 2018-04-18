package bfss3_test

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/bsm/bfs/bfss3"
	"github.com/bsm/bfs/testdata/lint"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

const (
	testAWSRegion  = "us-east-1"
	testBucketName = "bsm-bfs-unittest"
)

var _ = Describe("Bucket", func() {
	var data = lint.DefaultsData{}

	BeforeEach(func() {
		if os.Getenv("BFSS3_TEST") == "" {
			Skip("test is disabled, enable via BFSS3_TEST")
		}

		subject, err := bfss3.New(testBucketName, &bfss3.Config{
			Prefix: "x/" + strconv.FormatInt(time.Now().UnixNano(), 10),
			AWS:    aws.Config{Region: aws.String(testAWSRegion)},
		})
		Expect(err).NotTo(HaveOccurred())

		readonly, err := bfss3.New(testBucketName, &bfss3.Config{
			Prefix: "m/",
			AWS:    aws.Config{Region: aws.String(testAWSRegion)},
		})
		Expect(err).NotTo(HaveOccurred())

		data.Subject = subject
		data.Readonly = readonly
	})

	Context("defaults", lint.Defaults(&data))
})

// ------------------------------------------------------------------------

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "bfs/bfss3")
}

var _ = AfterSuite(func() {
	if os.Getenv("BFSS3_TEST") == "" {
		return
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(testAWSRegion),
	})
	Expect(err).NotTo(HaveOccurred())

	s3api := s3.New(sess)
	err = s3api.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket: aws.String(testBucketName),
		Prefix: aws.String("x"),
	}, func(page *s3.ListObjectsV2Output, _ bool) bool {
		for _, obj := range page.Contents {
			_, err := s3api.DeleteObject(&s3.DeleteObjectInput{
				Bucket: aws.String(testBucketName),
				Key:    obj.Key,
			})
			Expect(err).NotTo(HaveOccurred())
		}
		return true
	})
	Expect(err).NotTo(HaveOccurred())
})
