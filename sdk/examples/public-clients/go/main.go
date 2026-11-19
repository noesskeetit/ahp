// Standalone owner-client example. Run within an already claimed State lease.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	protocol "github.com/a2aproject/a2a-go/v2/a2a"
	runtime "github.com/ahp-platform/ahp/pkg/sdk"
	a2aclient "github.com/ahp-platform/ahp/sdk/go/a2a"
	"github.com/ahp-platform/ahp/sdk/go/compute"
	"github.com/ahp-platform/ahp/sdk/go/model"
	"github.com/ahp-platform/ahp/sdk/go/state"
	"github.com/ahp-platform/ahp/sdk/go/tools"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func env(name string) string {
	v := os.Getenv(name)
	if v == "" {
		panic("required configuration: " + name)
	}
	return v
}
func main() {
	family := flag.String("service", "model", "model, tools, compute or a2a")
	flag.Parse()
	if e := run(*family); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run(family string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cert, e := tls.LoadX509KeyPair(env("AHP_CERT_FILE"), env("AHP_KEY_FILE"))
	if e != nil {
		return errors.New("invalid mTLS identity")
	}
	ca, e := os.ReadFile(env("AHP_CA_FILE"))
	if e != nil {
		return errors.New("invalid trust bundle")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return errors.New("invalid trust bundle")
	}
	httpc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}}, Timeout: 2 * time.Minute}
	defer httpc.CloseIdleConnections()
	token := func(context.Context) (string, error) {
		raw, e := os.ReadFile(env("AHP_TOKEN_FILE"))
		if e != nil {
			return "", errors.New("current token unavailable")
		}
		return strings.TrimSpace(string(raw)), nil
	}
	base := env("AHP_API_URL")
	st, e := state.New(state.Config{BaseURL: base, HTTPClient: httpc, TokenSource: token})
	if e != nil {
		return e
	}
	epoch, e := strconv.ParseInt(env("AHP_EPOCH"), 10, 64)
	if e != nil {
		return errors.New("invalid lease epoch")
	}
	fence := state.Fence{RunID: env("AHP_RUN_ID"), Worker: env("AHP_WORKER"), Epoch: epoch}
	// The runtime supplies the durable task/node identity and persisted call identity.
	// A logical graph node label or a freshly minted tool-call ID is not sufficient.
	node, call := env("AHP_NODE_TASK_ID"), env("AHP_CALL_ID")
	recorder := runtime.NewIntentRecorder(st, fence, fence.RunID)
	var payload json.RawMessage = json.RawMessage(env("AHP_REQUEST_JSON"))
	intent, e := recorder.Record(ctx, node, call, family, "public-owner", payload)
	if e != nil {
		return e
	}
	current, e := st.GetIntent(ctx, fence.RunID, node, call)
	if e != nil {
		return e
	}
	if current.Status == state.IntentSucceeded {
		fmt.Println(string(current.Result))
		return nil
	}
	if current.Status != state.IntentRecorded {
		return fmt.Errorf("intent is %s; reconcile the existing operation, do not redispatch", current.Status)
	}
	if _, e = recorder.Dispatched(ctx, node, call); e != nil {
		return e
	}
	key := intent.ID // server-issued durable identity, stable across process restarts
	cfg := model.Config{BaseURL: base, HTTPClient: httpc, TokenSource: token}
	var result any
	switch family {
	case "model":
		c, err := model.New(cfg)
		if err != nil {
			return err
		}
		var req model.Request
		e = json.Unmarshal(payload, &req)
		if e == nil {
			req.Key = key
			result, e = c.Submit(ctx, req)
		}
	case "tools":
		c, err := tools.New(cfg)
		if err != nil {
			return err
		}
		var req struct {
			Tool      string          `json:"tool"`
			Arguments json.RawMessage `json:"arguments"`
		}
		e = json.Unmarshal(payload, &req)
		if e == nil {
			var op tools.Operation
			op, e = c.Call(ctx, key, req.Tool, req.Arguments)
			result = op
			if e == nil && op.Status != "SUCCEEDED" {
				e = fmt.Errorf("tool receipt %s; reconcile by original key", op.Status)
			}
		}
	case "compute":
		c, err := compute.New(compute.Config{BaseURL: base, HTTPClient: httpc, TokenSource: token})
		if err != nil {
			return err
		}
		var req struct {
			ComputerID string `json:"computer_id"`
			Command    string `json:"command"`
			Cwd        string `json:"cwd"`
		}
		e = json.Unmarshal(payload, &req)
		if e == nil {
			var command compute.Command
			command, e = c.Run(ctx, req.ComputerID, key, req.Command, req.Cwd)
			result = command
			if e == nil && command.Status != compute.CmdSucceeded {
				e = fmt.Errorf("command status %s; attach/reconcile original key", command.Status)
			}
		}
	case "a2a":
		c, err := a2aclient.New(cfg)
		if err != nil {
			return err
		}
		var req protocol.SendMessageRequest
		e = json.Unmarshal(payload, &req)
		if e == nil && req.Message != nil {
			req.Message.ID = key
			result, e = c.Send(ctx, &req)
		} else if e == nil {
			e = errors.New("message required")
		}
	default:
		e = errors.New("unknown service")
	}
	if e != nil {
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		defer stop()
		_, markErr := st.UpdateIntent(cleanup, fence, node, call, state.IntentUnknown, nil)
		return errors.Join(e, markErr)
	}
	raw, e := json.Marshal(result)
	if e != nil {
		return e
	}
	if _, e = recorder.Succeeded(ctx, node, call, raw); e != nil {
		return e
	}
	fmt.Println(string(raw))
	return nil
}
