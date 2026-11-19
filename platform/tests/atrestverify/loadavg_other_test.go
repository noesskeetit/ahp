//go:build !darwin

package atrestverify_test

func sysLoadAvg(out []float64) error { return unsupportedLoadAvg }
