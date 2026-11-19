package main

import (
	"path/filepath"
	"testing"
)

func TestNotesSurviveRestartAndNonceRetry(t *testing.T) {
	p := filepath.Join(t.TempDir(), "notes.jsonl")
	n, err := loadNotes(p)
	if err != nil {
		t.Fatal(err)
	}
	first, err := n.append("real durable note", "nonce")
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := loadNotes(p)
	if err != nil {
		t.Fatal(err)
	}
	again, err := restarted.append("real durable note", "nonce")
	if err != nil || again.ID != first.ID || len(restarted.items) != 1 {
		t.Fatalf("retry did not reuse durable note: %v", err)
	}
	if _, err := restarted.append("different", "nonce"); err == nil {
		t.Fatal("nonce reuse accepted")
	}
}
