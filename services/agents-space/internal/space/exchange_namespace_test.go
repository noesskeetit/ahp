package space

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestIngressAuthorityContextIsStableAndWorkloadScoped(t *testing.T) {
	var requests []exchangeRequest
	requestChannel := make(chan exchangeRequest, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var in exchangeRequest
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		requestChannel <- in
		// Actual Identity issues an attenuated child ID. The exchanger must
		// propagate that ID, rather than assuming it equals the request key.
		_ = json.NewEncoder(w).Encode(exchangeResponse{Token: "returned-task-token", Task: "issued-" + in.Task})
	}))
	defer server.Close()
	human := HumanPrincipal{Subject: "alice", HumanToken: "verified-human"}
	for _, namespace := range []string{"spiffe://ahp/tenant/t/deployment/dot-one", "spiffe://ahp/tenant/t/deployment/dot-two"} {
		exchanger, err := NewIdentityGrantExchanger(GrantExchangerConfig{IdentityExchangeURL: server.URL, Client: server.Client(), AuthorityNamespace: namespace})
		if err != nil {
			t.Fatal(err)
		}
		for range 2 {
			token, issued, err := exchanger.Exchange(t.Context(), human, "space-authority:alice::same-session")
			if err != nil {
				t.Fatal(err)
			}
			in := <-requestChannel
			requests = append(requests, in)
			if token != "returned-task-token" || issued != "issued-"+in.Task {
				t.Fatalf("issued authority not preserved: %v", err)
			}
		}
	}
	if requests[0].Task != requests[1].Task || requests[2].Task != requests[3].Task {
		t.Fatal("replay within one workload changed context")
	}
	if requests[0].Task == requests[2].Task {
		t.Fatal("two Dot workloads use one ingress context")
	}
	for _, in := range requests {
		if in.HumanToken != human.HumanToken || in.Context != "" {
			t.Fatal("namespace changed credentials or supplied conversation authority")
		}
	}
}
