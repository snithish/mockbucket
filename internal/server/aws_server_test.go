package server

import "testing"

func TestS3FrontendContract(t *testing.T) {
	// This checks that the S3 frontend satisfies the shared bucket, object, listing, and multipart contract.
	runFrontendContractTests(t, newS3ContractClient)
}
