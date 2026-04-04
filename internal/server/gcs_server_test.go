package server

import "testing"

func TestGCSFrontendContract(t *testing.T) {
	// This checks that the GCS frontend satisfies the shared bucket, object, listing, and multipart contract.
	runFrontendContractTests(t, newGCSContractClient)
}
