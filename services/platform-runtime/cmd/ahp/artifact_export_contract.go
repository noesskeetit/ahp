package main

import (
	"context"
	artifactsdk "github.com/ahp-platform/ahp/sdk/go/artifacts"
)

// artifactExportOwner crosses the co-located owner boundary without importing
// the Data tier into an untagged server or manufacturing a workload credential.
type artifactExportOwner interface {
	BeginUpload(context.Context, artifactsdk.UploadRequest) (artifactsdk.Upload, error)
	Upload(context.Context, artifactsdk.Upload, []byte) error
	Seal(context.Context, string) (artifactsdk.ArtifactRef, error)
	Get(context.Context, string) (artifactsdk.ArtifactRef, error)
}
