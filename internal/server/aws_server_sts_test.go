package server

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

func TestSTSAssumeRoleAndSessionCanHeadBucket(t *testing.T) {
	// This checks that AssumeRole returns usable session credentials for subsequent S3 requests.
	fixture := newAWSServerFixture(t, nil)
	stsClient := fixture.adminSTSClient(t)
	ctx := context.Background()

	stsOut, err := stsClient.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String("arn:mockbucket:iam:::role/data-reader"),
		RoleSessionName: aws.String("cli"),
	})
	if err != nil {
		t.Fatalf("AssumeRole() error = %v", err)
	}
	if stsOut.Credentials == nil || aws.ToString(stsOut.Credentials.AccessKeyId) == "" || aws.ToString(stsOut.Credentials.SessionToken) == "" {
		t.Fatalf("AssumeRole() missing credentials: %+v", stsOut.Credentials)
	}

	sessionClient := fixture.s3Client(
		t,
		aws.ToString(stsOut.Credentials.AccessKeyId),
		aws.ToString(stsOut.Credentials.SecretAccessKey),
		aws.ToString(stsOut.Credentials.SessionToken),
	)
	if _, err := sessionClient.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String("demo")}); err != nil {
		t.Fatalf("HeadBucket() with session error = %v", err)
	}
}
