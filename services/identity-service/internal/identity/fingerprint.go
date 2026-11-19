// Package identity: canonical body fingerprints.
//
// A fingerprint decides whether a repeated create under an existing task id
// is a replay (identical body) or a key reuse (different body). It is
// computed here rather than through contracts.Digest so the authority keeps a
// stable, dependency-light canonical form: every field is length-framed
// before it is hashed, so no boundary between two fields can be forged, and
// lists are hashed in their canonical (sorted, de-duplicated) order.

package identity

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// sortStrings sorts in place. Canonical capability order feeds both storage
// and fingerprints, so it is defined once here.
func sortStrings(v []string) { sort.Strings(v) }

// dedupe returns a sorted, de-duplicated copy and never mutates its input.
func dedupe(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, v := range in {
		if _, dup := seen[v]; dup {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// quote renders a caller-supplied token safely inside an error message.
func quote(v string) string { return strconv.Quote(v) }

// ttlSeconds renders a duration as whole seconds for make_interval, rounding
// up so a requested lifetime is never silently shortened.
func ttlSeconds(ttl time.Duration) float64 {
	secs := math.Ceil(ttl.Seconds())
	if secs < 1 {
		secs = 1
	}
	return secs
}

// digestAccumulator is a length-framed SHA-256 writer.
type digestAccumulator struct{ sum hash.Hash }

// newHash starts a fresh accumulator.
func newHash() *digestAccumulator { return &digestAccumulator{sum: sha256.New()} }

// str frames one field: an 8-byte big-endian length, then the bytes.
func (d *digestAccumulator) str(v string) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(len(v)))
	_, _ = d.sum.Write(b[:])
	_, _ = d.sum.Write([]byte(v))
}

// list frames the count and then every element, in the order given.
func (d *digestAccumulator) list(v []string) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(len(v)))
	_, _ = d.sum.Write(b[:])
	for _, s := range v {
		d.str(s)
	}
}

func (d *digestAccumulator) int(v int) { d.str("i:" + strconv.Itoa(v)) }

func (d *digestAccumulator) flag(v bool) {
	if v {
		d.str("b:1")
		return
	}
	d.str("b:0")
}

// dur rounds to whole seconds, matching what the database can store.
func (d *digestAccumulator) dur(v time.Duration) {
	d.str("d:" + strconv.FormatInt(int64(v.Round(time.Second)), 10))
}

func (d *digestAccumulator) hexSum() string {
	return contracts.DigestPrefix + hex.EncodeToString(d.sum.Sum(nil))
}

// rootDigest is the canonical body fingerprint of a CreateRoot call. The
// scope is part of it because the idempotency key is scope-bound. The limits
// must already be normalized: an omitted limit and an explicit default then
// digest identically, while any change to a ceiling or to AllowSelf is a
// different body and surfaces as KEY_REUSE on the same task id.
func rootDigest(scope contracts.Scope, r RootRequest, caps []string, ttl time.Duration,
	maxDepth, maxFanout, maxActive int, allowSelf bool) (string, error) {
	if len(caps) == 0 {
		return "", invalid("identity: root request must request at least one capability")
	}
	d := newHash()
	d.str("root")
	d.str(scope.Tenant)
	d.str(scope.Deployment)
	d.str(scope.Subject)
	d.str(r.Task)
	d.str(r.Context)
	d.str(r.Deployment)
	d.list(caps)
	d.dur(ttl)
	d.int(maxDepth)
	d.int(maxFanout)
	d.int(maxActive)
	d.flag(allowSelf)
	return d.hexSum(), nil
}

// delegateDigest is the canonical body fingerprint of a Delegate call. The
// parent task is included so the same child body under two different parents
// is never mistaken for a replay.
func delegateDigest(scope contracts.Scope, parentTask string, r DelegateRequest, caps []string, ttl time.Duration) (string, error) {
	if len(caps) == 0 {
		return "", invalid("identity: delegate request must request at least one capability")
	}
	d := newHash()
	d.str("delegate")
	d.str(scope.Tenant)
	d.str(scope.Deployment)
	d.str(scope.Subject)
	d.str(parentTask)
	d.str(r.Task)
	d.str(r.Context)
	d.str(r.Deployment)
	d.list(caps)
	d.dur(ttl)
	d.int(r.MaxDepth)
	d.int(r.MaxFanout)
	d.int(r.MaxActive)
	d.flag(r.AllowSelf)
	return d.hexSum(), nil
}
