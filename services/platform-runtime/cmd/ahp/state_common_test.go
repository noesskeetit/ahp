package main

import (
	"github.com/ahp-platform/ahp/internal/state"
	"testing"
)

func TestCommonStateModeAdmitsConfiguredNativeStorage(t *testing.T) {
	t.Setenv("AHP_STATE_COMMON_MODE", "interval")
	options, e := loadStateStoreOptions()
	if e != nil {
		t.Fatal(e)
	}
	s := state.New(nil, options...)
	if !s.CommonRecordsEnabled() {
		t.Fatal("COMMON_STATE_NOT_MOUNTED: configured interval mode still disables native storage")
	}
}
func TestCommonStateModeRejectsTyposBeforeStartup(t *testing.T) {
	t.Setenv("AHP_STATE_COMMON_MODE", "intervall")
	if _, e := loadStateStoreOptions(); e == nil {
		t.Fatal("invalid native storage configuration silently accepted")
	}
}
func TestCommonStateModeKeepsLegacyDefault(t *testing.T) {
	t.Setenv("AHP_STATE_COMMON_MODE", "")
	options, e := loadStateStoreOptions()
	if e != nil {
		t.Fatal(e)
	}
	if state.New(nil, options...).CommonRecordsEnabled() {
		t.Fatal("native storage silently enabled for legacy deployment")
	}
}

func TestCommonStateRejectsMissingReviewedNamespaceFile(t *testing.T) {
	t.Setenv("AHP_STATE_COMMON_MODE", "interval")
	t.Setenv("AHP_STATE_KV_NAMESPACE_POLICIES_FILE", t.TempDir()+"/absent.json")
	if _, e := loadStateStoreOptions(); e == nil {
		t.Fatal("reviewed namespace policy silently ignored")
	}
}

func TestCommonStateRejectsInvalidRetentionPolicyBeforeStartup(t *testing.T) {
	t.Setenv("AHP_STATE_COMMON_MODE", "interval")
	t.Setenv("AHP_STATE_COMMON_RETENTION_PROFILES_FILE", t.TempDir()+"/absent.json")
	if _, e := loadStateStoreOptions(); e == nil {
		t.Fatal("reviewed retention profile silently ignored")
	}
}

func TestCommonIncrementalModeRejectsSnapshotOrTypos(t *testing.T) {
	for _, mode := range []string{"", "off", "snapshot"} {
		t.Setenv("AHP_STATE_COMMON_MODE", mode)
		t.Setenv("AHP_STATE_COMMON_APPEND", "on")
		if _, e := loadStateStoreOptions(); e == nil {
			t.Fatal("incremental mode accepted incompatible reader", mode)
		}
	}
	t.Setenv("AHP_STATE_COMMON_MODE", "interval")
	t.Setenv("AHP_STATE_COMMON_APPEND", "typo")
	if _, e := loadStateStoreOptions(); e == nil {
		t.Fatal("incremental typo accepted")
	}
}
