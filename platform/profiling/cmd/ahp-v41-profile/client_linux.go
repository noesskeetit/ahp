package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/ahp-platform/ahp/pkg/contracts/journal"
	"golang.org/x/sys/unix"
)

func monotonicNS() int64 {
	var t unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC, &t); err != nil {
		panic(err)
	}
	return t.Nano()
}

type requestResult struct {
	ID               string      `json:"id"`
	Tenant           string      `json:"tenant"`
	Conversation     string      `json:"conversation"`
	SessionID        string      `json:"session_id,omitempty"`
	RequestedAt      int64       `json:"scheduled_ns,omitempty"`
	SentAt           int64       `json:"sent_ns"`
	AcceptedAt       int64       `json:"accepted_ns,omitempty"`
	FirstAt          int64       `json:"first_event_ns,omitempty"`
	SSEOpenedAt      int64       `json:"sse_opened_ns,omitempty"`
	SSEClosedAt      int64       `json:"sse_closed_ns,omitempty"`
	ClaimedAt        int64       `json:"worker_start_ns,omitempty"`
	CompletedAt      int64       `json:"terminal_sse_ns,omitempty"`
	VerifiedAt       int64       `json:"durable_verified_ns,omitempty"`
	BootNS           int64       `json:"vm_boot_ns,omitempty"`
	WarmHandoffNS    int64       `json:"warm_handoff_ns,omitempty"`
	PriorFinishNS    int64       `json:"prior_finish_rtt_ns,omitempty"`
	FenceNS          int64       `json:"fence_ns,omitempty"`
	Seq              int64       `json:"turn_seq,omitempty"`
	FenceTimeError   string      `json:"fence_failure,omitempty"`
	Failure          string      `json:"failure,omitempty"`
	Reused           *bool       `json:"reused_execution,omitempty"`
	Fault            bool        `json:"fault_scheduled,omitempty"`
	Tools            int         `json:"tools_requested"`
	Effects          int         `json:"mcp_effects_confirmed"`
	Large            bool        `json:"large_result,omitempty"`
	HTTPStatus       int         `json:"http_status,omitempty"`
	AdmissionMessage string      `json:"admission_message,omitempty"`
	FollowupCount    int         `json:"admission_followups,omitempty"`
	Executed         *turnOutput `json:"output,omitempty"`
}

type turnStart struct {
	ID                  string `json:"id"`
	ExecutionID         string `json:"execution_id"`
	Epoch               int64  `json:"epoch"`
	Mono                int64  `json:"start_monotonic_ns"`
	WarmHandoffNS       int64  `json:"warm_handoff_ns,omitempty"`
	PreviousFinishRTTNS int64  `json:"previous_finish_rtt_ns,omitempty"`
}

type inflight struct {
	result requestResult
	path   string
	token  string
}

func (s *stand) postTurn(ctx context.Context, index int, conversation, delivery string, input turnInput, scheduled int64) inflight {
	scope := s.Scope(index)
	call := inflight{
		result: requestResult{ID: input.ID, Tenant: scope.Tenant, Conversation: conversation, RequestedAt: scheduled,
			SessionID: journal.ScopedSessionID(scope, conversation), Tools: input.Tools, Fault: input.Fault, Large: input.Large},
		path:  s.CoordinatorURL + "/v1/agents/" + url.PathEscape(scope.Deployment) + "/sessions/" + url.PathEscape(conversation),
		token: "",
	}
	// Token refresh belongs to the load client, outside the timed platform call.
	token, err := s.SubjectToken(ctx, index)
	if err != nil {
		call.result.Failure = "identity_browser_token"
		return call
	}
	call.token = token
	payload, err := json.Marshal(input)
	if err != nil {
		call.result.Failure = "marshal_input"
		return call
	}
	body, err := json.Marshal(journal.EnqueueRequest{
		Harness: "go", FormatVersion: 1, Key: input.ID, DeliveryMode: delivery,
		Input: journal.Payload{Inline: payload},
	})
	if err != nil {
		call.result.Failure = "marshal_admission"
		return call
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, call.path+":send", bytes.NewReader(body))
	if err != nil {
		call.result.Failure = "invalid_url"
		return call
	}
	req.Header.Set("Authorization", "Bearer "+call.token)
	req.Header.Set("Content-Type", "application/json")
	call.result.SentAt = monotonicNS()
	resp, err := s.CoordinatorHTTP.Do(req)
	if err != nil {
		call.result.Failure = "admission_transport"
		return call
	}
	defer resp.Body.Close()
	call.result.HTTPStatus = resp.StatusCode
	var receipt journal.EnqueueReceipt
	if resp.StatusCode != http.StatusAccepted {
		var code struct {
			Code    string `json:"code"`
			Reason  string `json:"reason"`
			Message string `json:"message"`
		}
		_ = json.NewDecoder(io.LimitReader(resp.Body, 1<<12)).Decode(&code)
		call.result.AdmissionMessage = code.Message
		call.result.Failure = "admission_http_" + strconv.Itoa(resp.StatusCode) + "/" + code.Code + "/" + code.Reason
		return call
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<12)).Decode(&receipt); err != nil || receipt.HeadSeq < 1 {
		call.result.Failure = "invalid_admission_receipt"
		return call
	}
	call.result.Seq = receipt.HeadSeq
	call.result.FollowupCount = receipt.FollowupCount
	call.result.AcceptedAt = monotonicNS()
	return call
}

func (s *stand) readTurn(ctx context.Context, call inflight, firecracker bool) requestResult {
	return s.readTurnWithHandoff(ctx, call, firecracker, nil)
}

func (s *stand) readTurnWithHandoff(ctx context.Context, call inflight, firecracker bool, onDelta func()) (r requestResult) {
	r = call.result
	if r.Failure != "" {
		return r
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, call.path+"/stream?turn_seq="+strconv.FormatInt(r.Seq, 10), nil)
	if err != nil {
		r.Failure = "invalid_stream_url"
		return r
	}
	req.Header.Set("Authorization", "Bearer "+call.token)
	resp, err := s.CoordinatorHTTP.Do(req)
	if err != nil {
		r.Failure = "stream_transport"
		return r
	}
	defer func() {
		_ = resp.Body.Close()
		if r.SSEOpenedAt > 0 {
			r.SSEClosedAt = monotonicNS()
		}
	}()
	if resp.StatusCode != http.StatusOK {
		r.Failure = "stream_http_" + strconv.Itoa(resp.StatusCode)
		return r
	}
	r.SSEOpenedAt = monotonicNS()

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 8192), 2<<20)
	var kind string
	var lastSeq int64
	var terminal journal.TurnCompleted
	var started turnStart
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "event: "):
			kind = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			body := []byte(strings.TrimPrefix(line, "data: "))
			switch kind {
			case "journal":
				if r.FirstAt == 0 {
					r.FirstAt = monotonicNS()
				}
				var event journal.Event
				if json.Unmarshal(body, &event) != nil || event.TurnSeq != r.Seq || event.Seq <= lastSeq {
					r.Failure = "invalid_sse_order"
					return r
				}
				lastSeq = event.Seq
				switch event.Type {
				case "bench.started":
					var eventStart turnStart
					if json.Unmarshal(event.Payload.Inline, &eventStart) != nil || eventStart.ID != r.ID {
						r.Failure = "invalid_worker_start"
						return r
					}
					started = eventStart
					if !firecracker && r.ClaimedAt == 0 {
						r.ClaimedAt = started.Mono
					}
					r.WarmHandoffNS, r.PriorFinishNS = started.WarmHandoffNS, started.PreviousFinishRTTNS
				case "bench.delta":
					if onDelta != nil {
						onDelta()
						onDelta = nil
					}
				case "turn.output":
					var out turnOutput
					if json.Unmarshal(event.Payload.Inline, &out) != nil || out.ID != r.ID {
						r.Failure = "unexpected_turn_output"
						return r
					}
					r.Executed = &out
				case "turn.completed":
					if json.Unmarshal(event.Payload.Inline, &terminal) != nil || terminal.TurnSeq != r.Seq || terminal.EndSeq != event.Seq {
						r.Failure = "invalid_terminal_journal"
						return r
					}
				}
			case "error":
				var v struct{ Code string }
				_ = json.Unmarshal(body, &v)
				r.Failure = "sse_" + v.Code
				return r
			case "end":
				var end journal.TurnCompleted
				if json.Unmarshal(body, &end) != nil || end.TurnSeq != r.Seq || end.EndSeq != terminal.EndSeq ||
					end.EndSeq <= 0 || end.Outcome != "IDLE" || r.Executed == nil || started.ID != r.ID {
					r.Failure = "invalid_sse_terminal"
					return r
				}
				r.CompletedAt = monotonicNS()
				if r.Large && r.Executed.ArtifactResults != 1 {
					r.Failure = "missing_artifact_descriptor"
					return r
				}
				for i := 0; i < r.Tools; i++ {
					n := s.Effects.count(fmt.Sprintf("%s/%d", r.ID, i))
					r.Effects += n
					if n != 1 {
						r.Failure = "mcp_effect_count"
					}
				}
				if r.Failure != "" {
					return r
				}
				r.VerifiedAt = monotonicNS()
				return r
			}
		}
	}
	r.Failure = "sse_early_eof"
	if scanner.Err() != nil || ctx.Err() != nil {
		r.Failure = "sse_interrupted"
	}
	return r
}

func withTurnTimeout(ctx context.Context, timeout time.Duration, fn func(context.Context) requestResult) requestResult {
	turnCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return fn(turnCtx)
}
