package state

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"unicode/utf8"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

const SharedValuesFormat = "ahp-shared-values/1"
const SharedChunkFormat = "ahp-shared-chunk/1"
const SharedChunkBytes = 192 << 10
const SharedMaxValueBytes = 16 << 20
const SharedMaxTotalBytes = 32 << 20
const SharedMaxKeys = 256

type SharedValueDescriptor struct {
	Key     string   `json:"key"`
	Version int64    `json:"version"`
	SHA256  string   `json:"sha256"`
	Size    int      `json:"size"`
	Chunks  []string `json:"chunks"`
}
type SharedManifest struct {
	Format  string                  `json:"format"`
	Binding []string                `json:"binding"`
	Values  []SharedValueDescriptor `json:"values"`
}
type sharedChunk struct {
	Format string `json:"format"`
	Bytes  string `json:"bytes_base64"`
}

// SharedValues never changes scope: Namespace must already be reviewed by the
// State owner. Prepared writes are published atomically by CommitRecords.
type SharedValues struct {
	Client    *Client
	Namespace []string
	Owner     string
	Key       string
	Binding   []string
}

func sharedHash(raw []byte) string { h := sha256.Sum256(raw); return hex.EncodeToString(h[:]) }
func sharedValidHash(h string) bool {
	raw, e := hex.DecodeString(h)
	return e == nil && len(raw) == 32 && hex.EncodeToString(raw) == h
}
func (s SharedValues) readKey(key string) KVRead {
	return KVRead{Namespace: s.Namespace, ExpectedOwner: s.Owner, Key: key}
}
func (s SharedValues) validate() error {
	if s.Client == nil || len(s.Namespace) == 0 || s.Key == "" || (s.Owner != "subject" && s.Owner != "deployment") {
		return newInvalid("shared data requires explicit reviewed subject/deployment owner")
	}
	return nil
}
func (s SharedValues) manifest(item *KVItem) (SharedManifest, error) {
	binding := append([]string{}, s.Binding...)
	m := SharedManifest{Format: SharedValuesFormat, Binding: binding, Values: []SharedValueDescriptor{}}
	if item == nil {
		return m, nil
	}
	d := json.NewDecoder(bytes.NewReader(item.Value))
	d.DisallowUnknownFields()
	if e := d.Decode(&m); e != nil {
		return m, newInvalid("invalid shared manifest")
	}
	a, _ := json.Marshal(m.Binding)
	b, _ := json.Marshal(binding)
	if m.Format != SharedValuesFormat || !bytes.Equal(a, b) || m.Values == nil || len(m.Values) > SharedMaxKeys {
		return m, newInvalid("incompatible shared manifest or key bound")
	}
	seen := map[string]bool{}
	total := 0
	for _, v := range m.Values {
		key, e := base64.StdEncoding.DecodeString(v.Key)
		if e != nil || !utf8.Valid(key) || len(key) > 1024 || seen[string(key)] || v.Version < 1 || v.Size < 1 || v.Size > SharedMaxValueBytes || len(v.Chunks) != (v.Size+SharedChunkBytes-1)/SharedChunkBytes || !sharedValidHash(v.SHA256) {
			return m, newInvalid("invalid shared value descriptor")
		}
		seen[string(key)] = true
		for _, h := range v.Chunks {
			if !sharedValidHash(h) {
				return m, newInvalid("invalid shared chunk digest")
			}
		}
		total += v.Size
	}
	if total > SharedMaxTotalBytes || len(item.Value) > 512<<10 {
		return m, newInvalid("shared owner byte bound")
	}
	return m, nil
}
func sharedDecodeChunk(item *KVItem, h string) ([]byte, error) {
	if item == nil {
		return nil, newInvalid("missing shared immutable chunk")
	}
	var c sharedChunk
	d := json.NewDecoder(bytes.NewReader(item.Value))
	d.DisallowUnknownFields()
	if e := d.Decode(&c); e != nil || c.Format != SharedChunkFormat {
		return nil, newInvalid("invalid shared chunk envelope")
	}
	raw, e := base64.StdEncoding.DecodeString(c.Bytes)
	if e != nil || len(raw) < 1 || len(raw) > SharedChunkBytes || sharedHash(raw) != h {
		return nil, newInvalid("shared chunk integrity mismatch")
	}
	return raw, nil
}
func (s SharedValues) readManifest(ctx context.Context) (*KVItem, SharedManifest, error) {
	if e := s.validate(); e != nil {
		return nil, SharedManifest{}, e
	}
	items, e := s.Client.GetKVBatch(ctx, []KVRead{s.readKey(s.Key)})
	if e != nil {
		return nil, SharedManifest{}, e
	}
	m, e := s.manifest(items[0])
	return items[0], m, e
}
func (s SharedValues) Read(ctx context.Context) (map[string]json.RawMessage, *KVItem, error) {
	item, m, e := s.readManifest(ctx)
	if e != nil {
		return nil, nil, e
	}
	hashes := map[string]bool{}
	for _, v := range m.Values {
		for _, h := range v.Chunks {
			hashes[h] = true
		}
	}
	keys := []string{}
	for h := range hashes {
		keys = append(keys, h)
	}
	sort.Strings(keys)
	chunks := map[string][]byte{}
	for start := 0; start < len(keys); start += 8 {
		end := min(start+8, len(keys))
		reads := []KVRead{}
		for _, h := range keys[start:end] {
			reads = append(reads, s.readKey("blob-"+h))
		}
		rows, e := s.Client.GetKVBatch(ctx, reads)
		if e != nil {
			return nil, nil, e
		}
		for i, row := range rows {
			h := keys[start+i]
			raw, e := sharedDecodeChunk(row, h)
			if e != nil {
				return nil, nil, e
			}
			chunks[h] = raw
		}
	}
	values := map[string]json.RawMessage{}
	for _, v := range m.Values {
		raw := make([]byte, 0, v.Size)
		for _, h := range v.Chunks {
			raw = append(raw, chunks[h]...)
		}
		if len(raw) != v.Size || sharedHash(raw) != v.SHA256 || !json.Valid(raw) {
			return nil, nil, newInvalid("shared value integrity mismatch")
		}
		key, _ := base64.StdEncoding.DecodeString(v.Key)
		values[string(key)] = raw
	}
	return values, item, nil
}

// Prepare stages immutable chunks before returning a CAS-protected manifest.
// A transport error is returned without retry; no manifest is published here.
func (s SharedValues) Prepare(ctx context.Context, f Fence, changes map[string]json.RawMessage) (KVPut, error) {
	item, m, e := s.readManifest(ctx)
	if e != nil {
		return KVPut{}, e
	}
	entries := map[string]SharedValueDescriptor{}
	for _, v := range m.Values {
		entries[v.Key] = v
	}
	chunks := map[string][]byte{}
	for key, value := range changes {
		if !utf8.ValidString(key) || len(value) > SharedMaxValueBytes || !utf8.Valid(value) || !json.Valid(value) {
			return KVPut{}, newInvalid("shared value must be bounded UTF-8 JSON")
		}
		// Preserve JSON bytes; hashes do not depend on a Go/Python float re-encoding.
		raw := []byte(value)
		encoded := base64.StdEncoding.EncodeToString([]byte(key))
		v := SharedValueDescriptor{Key: encoded, Version: entries[encoded].Version + 1, SHA256: sharedHash(raw), Size: len(raw), Chunks: []string{}}
		for start := 0; start < len(raw); start += SharedChunkBytes {
			chunk := raw[start:min(start+SharedChunkBytes, len(raw))]
			h := sharedHash(chunk)
			v.Chunks = append(v.Chunks, h)
			chunks[h] = chunk
		}
		entries[encoded] = v
	}
	names := []string{}
	for key := range entries {
		names = append(names, key)
	}
	sort.Strings(names)
	m.Values = []SharedValueDescriptor{}
	for _, key := range names {
		m.Values = append(m.Values, entries[key])
	}
	body, e := json.Marshal(m)
	if e != nil {
		return KVPut{}, e
	}
	if _, e = s.manifest(&KVItem{Value: body}); e != nil {
		return KVPut{}, e
	}
	hashes := []string{}
	for h := range chunks {
		hashes = append(hashes, h)
	}
	sort.Strings(hashes)
	for start := 0; start < len(hashes); start += 8 {
		group := hashes[start:min(start+8, len(hashes))]
		reads := []KVRead{}
		for _, h := range group {
			reads = append(reads, s.readKey("blob-"+h))
		}
		rows, e := s.Client.GetKVBatch(ctx, reads)
		if e != nil {
			return KVPut{}, e
		}
		puts := []KVPut{}
		for i, row := range rows {
			h := group[i]
			if row != nil {
				if _, e = sharedDecodeChunk(row, h); e != nil {
					return KVPut{}, e
				}
				continue
			}
			value, _ := json.Marshal(sharedChunk{Format: SharedChunkFormat, Bytes: base64.StdEncoding.EncodeToString(chunks[h])})
			puts = append(puts, KVPut{Namespace: s.Namespace, Key: "blob-" + h, ExpectedOwner: s.Owner, Value: value})
		}
		if len(puts) == 0 {
			continue
		}
		op, _ := json.Marshal([]any{f.RunID, puts})
		_, e = s.Client.PutKVBatch(ctx, f, "stage-"+sharedHash(op), puts)
		if e != nil {
			if contracts.Code(e) != contracts.ErrConflict {
				return KVPut{}, e
			}
			rows, e = s.Client.GetKVBatch(ctx, reads)
			if e != nil {
				return KVPut{}, e
			}
			for i, row := range rows {
				if _, e = sharedDecodeChunk(row, group[i]); e != nil {
					return KVPut{}, fmt.Errorf("concurrent shared staging: %w", e)
				}
			}
		}
	}
	version := int64(0)
	if item != nil {
		version = item.Version
	}
	return KVPut{Namespace: s.Namespace, Key: s.Key, ExpectedOwner: s.Owner, ExpectedVersion: version, Value: body}, nil
}
