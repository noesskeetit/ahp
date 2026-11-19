//go:build !linux

package compute

// kvmVMCreateProblem has nothing to ask where there is no KVM node: every
// caller refuses those hosts on the OS or permission gate before reaching the
// kernel question, so there is no additional reason to report.
func kvmVMCreateProblem() string { return "" }
