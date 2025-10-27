package bfss3_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/bsm/bfs/bfss3"
	"github.com/bsm/bfs/testdata/lint"
)

func Test(t *testing.T) {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithBaseEndpoint("http://127.0.0.1:4566"),
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     "test",
				SecretAccessKey: "test",
			},
		}))
	if err != nil {
		t.Fatal("Unexpected error", err)
	}

	ctx := context.Background()
	bucket, err := bfss3.New(ctx, "bfs-s3-test", &bfss3.Config{AWS: &cfg})
	if err != nil {
		t.Fatal("Unexpected error", err)
	}
	defer bucket.Close()

	t.Run("common", func(t *testing.T) {
		lint.Common(t, bucket, lint.Supports{ContentType: true, Metadata: true})
	})

	t.Run("slow", func(t *testing.T) {
		lint.Slow(t, bucket, lint.Supports{ContentType: true, Metadata: true})
	})
}
