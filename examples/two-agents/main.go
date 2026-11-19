// Command two-agents demonstrates the Agents Space SDK driving two separate
// Space deployments and an agent-to-agent delegation between them.
//
// It is a pure client: it talks to two already-running as-runner deployments
// (one per "agent") over their public /v1/space/ API. Nothing here embeds a
// State service or an LLM — those live behind each deployment. The example
// shows the exact human/developer journey:
//
//  1. Accept a turn on the orchestrator deployment (A).
//  2. Ask A to run (Dispatch) and follow its stream to a terminal status.
//  3. Delegate a follow-up turn from A to a second deployment (B).
//  4. Run B, follow its stream, and print the resulting thread/turn ids.
//
// Configuration comes only from the environment (see README). Secrets (tokens)
// are read from env and never printed.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/ahp-platform/ahp/sdk/go/agent"
)

type deploymentEnv struct {
	Name  string
	URL   string
	Token string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "two-agents:", err)
		os.Exit(1)
	}
}

func run() error {
	a, err := loadDeployment("A")
	if err != nil {
		return err
	}
	b, err := loadDeployment("B")
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	clientA := agent.NewClient(agent.Config{BaseURL: a.URL, Token: a.Token})
	clientB := agent.NewClient(agent.Config{BaseURL: b.URL, Token: b.Token})

	// 1. Accept a turn on the orchestrator deployment A.
	outA, err := clientA.AcceptTurn(ctx, agent.TurnInput{
		Key:       fmt.Sprintf("two-agents-a-%d", time.Now().UnixNano()),
		ContextID: "example/two-agents",
		Input:     json.RawMessage(`{"role":"user","content":"plan the work"}`),
		MaxSteps:  8,
	})
	if err != nil {
		return fmt.Errorf("accept turn on %s: %w", a.Name, err)
	}
	fmt.Printf("[%s] accepted turn=%s thread=%s status=%s\n", a.Name, outA.TurnID, outA.ThreadID, outA.Status)

	// 2. Run A and follow its stream until it settles.
	if err := clientA.Dispatch(ctx); err != nil {
		return fmt.Errorf("dispatch %s: %w", a.Name, err)
	}
	if err := awaitTerminal(ctx, clientA, outA.ThreadID); err != nil {
		return fmt.Errorf("await %s: %w", a.Name, err)
	}
	fmt.Printf("[%s] turn=%s reached terminal status\n", a.Name, outA.TurnID)

	// 3. Delegate a follow-up from A to B. The delegation hands a fresh turn
	//    to B's AcceptTurn; B owns its own scope and thread.
	outB, err := clientA.Delegate(ctx, clientB, agent.TurnInput{
		Key:       fmt.Sprintf("two-agents-b-%d", time.Now().UnixNano()),
		ContextID: "example/two-agents/delegated",
		Input:     json.RawMessage(`{"role":"user","content":"execute the delegated subtask"}`),
		MaxSteps:  8,
	})
	if err != nil {
		return fmt.Errorf("delegate %s->%s: %w", a.Name, b.Name, err)
	}
	fmt.Printf("[%s] delegated turn=%s thread=%s status=%s\n", b.Name, outB.TurnID, outB.ThreadID, outB.Status)

	// 4. Run B and follow its stream.
	if err := clientB.Dispatch(ctx); err != nil {
		return fmt.Errorf("dispatch %s: %w", b.Name, err)
	}
	if err := awaitTerminal(ctx, clientB, outB.ThreadID); err != nil {
		return fmt.Errorf("await %s: %w", b.Name, err)
	}
	fmt.Printf("[%s] turn=%s reached terminal status\n", b.Name, outB.TurnID)

	if outA.TurnID == outB.TurnID || outA.ThreadID == outB.ThreadID {
		return fmt.Errorf("deployments collided: turn/thread ids not distinct")
	}
	fmt.Println("OK: two independent deployments, distinct turns/threads, delegation succeeded")
	return nil
}

// awaitTerminal polls ReadStream for a terminal "status" event on the thread.
func awaitTerminal(ctx context.Context, c *agent.Client, threadID string) error {
	after := int64(0)
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		raw, err := c.ReadStream(ctx, threadID, after, 100)
		if err != nil {
			return err
		}
		var events []struct {
			Seq     int64           `json:"seq"`
			Kind    string          `json:"kind"`
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(raw, &events); err != nil {
			return fmt.Errorf("decode stream: %w", err)
		}
		for _, ev := range events {
			if ev.Seq > after {
				after = ev.Seq
			}
			if ev.Kind == "status" {
				var s struct {
					Status string `json:"status"`
				}
				_ = json.Unmarshal(ev.Payload, &s)
				switch s.Status {
				case "done", "failed", "canceled":
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("timeout waiting for terminal status on thread %s", threadID)
}

func loadDeployment(name string) (deploymentEnv, error) {
	url := os.Getenv("AS_" + name + "_URL")
	if url == "" {
		return deploymentEnv{}, fmt.Errorf("AS_%s_URL is required (deployment %s /v1/space base URL)", name, name)
	}
	return deploymentEnv{
		Name:  name,
		URL:   url,
		Token: os.Getenv("AS_" + name + "_TOKEN"),
	}, nil
}
