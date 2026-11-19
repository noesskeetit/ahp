package state

import (
	"context"
	"encoding/json"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"net/http"
	"sync/atomic"
	"testing"
)

func TestCommonSDKPublishesDeltaWithFenceAndReadsFreshBundle(t *testing.T) {
	m := newTestTLS(t)
	var calls atomic.Int32
	var tokens atomic.Int32
	srv := newServer(t, m, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var in map[string]json.RawMessage
		if e := json.NewDecoder(r.Body).Decode(&in); e != nil {
			t.Error(e)
		}
		if r.Header.Get("Authorization") == "" {
			t.Error("missing current task credential")
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/state/CommitRecords":
			if in["document"] != nil || in["delta"] == nil {
				t.Error("delta accidentally sent full/empty document")
			}
			var f Fence
			if e := json.Unmarshal(in["fence"], &f); e != nil || f.RunID != "r" || f.Worker != "w" || f.Epoch != 7 {
				t.Error("fence lost", e)
			}
			w.Write([]byte(`{"cut_id":"c1","run_id":"r","namespace":"native","revision":1,"key":"event1","record_count":1,"kv":[{"namespace":["app"],"key":"state","version":3,"value":{"v":3}}]}`))
		case "/v1/state/ReadRecordsBundle":
			if string(in["known_revision"]) != "1" {
				t.Error("cache hint lost")
			}
			w.Write([]byte(`{"common":null,"common_unchanged":true,"receipt":{"cut_id":"c1","revision":1},"kv":[{"namespace":["app"],"key":"state","version":4,"value":{"v":4}}]}`))
		default:
			t.Error("unexpected route", r.URL.Path)
			w.WriteHeader(404)
		}
	})
	c := clientFor(t, m, srv.URL, func(context.Context) (string, error) {
		n := tokens.Add(1)
		if n == 1 {
			return "first", nil
		}
		return "second", nil
	})
	out, e := c.CommitRecords(context.Background(), CommonCommit{Fence: Fence{RunID: "r", Worker: "w", Epoch: 7}, Namespace: "native", Key: "event1", Delta: &CommonDelta{Format: CommonFormat, Profile: "test/1", Root: "s", Put: map[string]CommonRecord{"s": {Kind: "session", Attributes: json.RawMessage(`{}`), Relations: map[string][]string{}}}}})
	if e != nil || out.Revision != 1 || out.KV[0].Version != 3 {
		t.Fatal("common commit", e)
	}
	b, e := c.ReadRecordsBundle(context.Background(), "t", "native", []KVRead{{Namespace: []string{"app"}, Key: "state", ExpectedOwner: "deployment"}}, 1)
	if e != nil || !b.Unchanged || b.Common != nil || b.Receipt.CutID != "c1" || b.KV[0].Version != 4 {
		t.Fatal("fresh bundle", e)
	}
	if calls.Load() != 2 || tokens.Load() != 2 {
		t.Fatal("unexpected retry or token reuse")
	}
}
func TestCommonSDKRefusesAmbiguousPublicationBeforeHTTP(t *testing.T) {
	m := newTestTLS(t)
	var calls atomic.Int32
	srv := newServer(t, m, func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.Write([]byte(`{}`)) })
	c := clientFor(t, m, srv.URL, staticToken("t"))
	for _, req := range []CommonCommit{{Fence: Fence{RunID: "r", Worker: "w", Epoch: 1}, Key: "k"}, {Fence: Fence{RunID: "r", Worker: "w", Epoch: 1}, Key: "k", Document: &CommonDocument{}, Delta: &CommonDelta{}}} {
		if _, e := c.CommitRecords(context.Background(), req); contracts.Code(e) != contracts.ErrInvalid {
			t.Fatal("ambiguous request was not invalid", e)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid publication reached HTTP")
	}
}
