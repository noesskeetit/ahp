//go:build r4_open_measure

// This file carries the r4_open_measure build constraint for the R4-OPEN
// measurement legs. Their content lives in delegate_ssi_realpath_test.go (which
// is compiled only under that tag) and the abort-tracing fixture they share with
// the default availability legs lives in r4_wire_trace_test.go, compiled by every
// build so both modes measure with one definition.
package identity
