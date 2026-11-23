//go:build unix

package vmruntime

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

const injectorTestToolLease = `{"session_id":"session/alpha","lease_epoch":4,"tool_lease_proof":"signed-proof","key":"call-1"}`

func TestInjectorForwardsOnlyLeasedToolRoutes(t *testing.T) {
	guest := injectorTestGuest()
	token := injectorTestJWT(time.Now().Add(5 * time.Minute))
	var toolsClients, mints atomic.Int64
	var redirected atomic.Bool
	redirect := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		redirected.Store(true)
	}))
	defer redirect.Close()
	type request struct{ path, body, token, cookie, forwarded string }
	received := make(chan request, 4)
	tools := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read forwarded body: %v", err)
		}
		received <- request{r.RequestURI, string(body), r.Header.Get("Authorization"), r.Header.Get("Cookie"), r.Header.Get("X-Forwarded-For")}
		w.Header().Set("Set-Cookie", "gateway-secret=1")
		if r.URL.Path == "/v1/tools/ConnectionState" && string(body) == " {} " {
			http.Redirect(w, r, redirect.URL, http.StatusTemporaryRedirect)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer tools.Close()
	config := InjectorConfig{
		StateURL: "https://state.invalid",
		ToolsURL: tools.URL,
		ClientFor: func(context.Context, contracts.Scope) (*http.Client, error) {
			t.Error("tool request selected State API credentials")
			return nil, errors.New("wrong upstream")
		},
		ToolsClientFor: func(_ context.Context, scope contracts.Scope) (*http.Client, error) {
			if scope != guest.Execution.Scope {
				t.Errorf("unexpected workload scope: %v", scope)
			}
			toolsClients.Add(1)
			return tools.Client(), nil
		},
		MintToken: func(_ context.Context, exec Execution) (string, error) {
			if exec != guest.Execution {
				t.Errorf("unexpected mint execution: %v", exec)
			}
			mints.Add(1)
			return token, nil
		},
	}
	call := strings.TrimSuffix(injectorTestToolLease, "}") + `, "tool":"echo", "arguments" : {"z": 2, "a": [1]}}` + "\n"
	for _, mode := range []struct {
		name     string
		disabled bool
	}{{"enabled", false}, {"disabled", true}} {
		t.Run(mode.name, func(t *testing.T) {
			cfg := config
			if mode.disabled {
				cfg.ToolsURL = ""
			}
			i, err := NewInjector(cfg)
			if err != nil {
				t.Fatal(err)
			}
			socket := injectorTestSocket(t)
			if err := i.Start(t.Context(), guest, socket); err != nil {
				t.Fatal(err)
			}
			defer i.Close(guest)
			client := injectorTestClient(socket)
			defer client.CloseIdleConnections()
			beforeClients, beforeMints := toolsClients.Load(), mints.Load()
			denied := []struct{ method, operation, body string }{
				{"GET", "ConnectionState", ""},
				{"POST", "ConnectionState?", "{}"},
				{"POST", "ConnectionState?probe=1", "{}"},
				{"POST", "ConnectionState", `{"proof":"proof"}`},
				{"POST", "ConnectionState", "null"},
				{"POST", "ConnectionState", "{}" + strings.Repeat(" ", 4095)},
				{"POST", "Call", strings.Replace(call, "session/alpha", "sibling", 1)},
				{"POST", "Call", strings.Replace(call, `"lease_epoch":4`, `"lease_epoch":2`, 1)},
				{"POST", "Call", strings.Replace(call, "signed-proof", "", 1)},
				{"POST", "Call", strings.Replace(call, "call-1", "", 1)},
				{"POST", "Call", strings.Replace(call, "echo", "", 1)},
				{"POST", "Call", strings.TrimSuffix(injectorTestToolLease, "}") + `,"tool":"echo"}`},
				{"POST", "Call", strings.Replace(call, "session_id", "Session_ID", 1)},
				{"POST", "Call", strings.Replace(call, "session_id", "ſession_id", 1)},
				{"POST", "Call", strings.Replace(call, `"key":"call-1"`, `"key":"first","key":"call-1"`, 1)},
				{"POST", "Call", call + strings.Repeat(" ", 1<<20)},
				{"POST", "FenceAndLookup", call},
				{"POST", "FenceAndLookup", strings.Replace(injectorTestToolLease, "signed-proof", "", 1)},
			}
			for _, operation := range []string{"Approve", "Decline", "Approvals", "OAuthConnections", "OAuthBegin", "OAuthCallback", "OAuthRevoke", "Get", "GetResult", "Fence", "%43all"} {
				denied = append(denied, struct{ method, operation, body string }{"POST", operation, call})
			}
			for _, req := range denied {
				resp, _ := injectorTestRequest(t, client, req.method, "/v1/tools/"+req.operation, req.body)
				if resp.StatusCode != http.StatusForbidden {
					t.Fatalf("%s %s: status = %d, want 403", req.method, req.operation, resp.StatusCode)
				}
			}
			if toolsClients.Load() != beforeClients || mints.Load() != beforeMints || len(received) != 0 {
				t.Fatal("denied request minted a token or selected an upstream")
			}
			for _, req := range []struct{ operation, body string }{
				{"Call", call}, {"FenceAndLookup", injectorTestToolLease}, {"ConnectionState", "{}"}, {"ConnectionState", " {} "},
			} {
				path := "/v1/tools/" + req.operation
				resp, _ := injectorTestRequest(t, client, http.MethodPost, path, req.body)
				wantStatus := http.StatusAccepted
				if mode.disabled {
					wantStatus = http.StatusForbidden
				} else if req.body == " {} " {
					wantStatus = http.StatusServiceUnavailable
				}
				if resp.StatusCode != wantStatus || resp.Header.Get("Set-Cookie") != "" {
					t.Fatalf("%s: status = %d, want %d without upstream cookies", path, resp.StatusCode, wantStatus)
				}
				if !mode.disabled {
					want := request{path, req.body, "Bearer " + token, "", ""}
					select {
					case got := <-received:
						if got != want {
							t.Fatalf("forwarded request diverged from fixed route, raw body or host credentials: %q", got.path)
						}
					default:
						t.Fatal("no forwarded request")
					}
				}
			}
			if mode.disabled && (toolsClients.Load() != beforeClients || mints.Load() != beforeMints || len(received) != 0) {
				t.Fatal("disabled tools routes reached token mint or upstream")
			}
		})
	}
	if toolsClients.Load() != 4 || mints.Load() != 1 || redirected.Load() {
		t.Fatal("tool routes did not use one cached mint and reject redirects")
	}
}
