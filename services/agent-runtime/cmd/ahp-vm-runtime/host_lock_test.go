//go:build unix

package main

import "testing"

func TestOnlyOneRuntimeOwnsTheHostSlots(t *testing.T) {
	dir := t.TempDir()
	first, err := lockHostSlots(dir)
	if err != nil {
		t.Fatal(err)
	}
	if second, err := lockHostSlots(dir); err == nil {
		_ = second.Close()
		t.Fatal("two runtimes concurrently own the same injector sockets")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	next, err := lockHostSlots(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Close()
}
