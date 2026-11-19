package gateway

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/a2aproject/a2a-go/v2/a2a"
)

// The gateway serves the A2A core protocol through the official transport
// (a2asrv.NewJSONRPCHandler). a2a-go v2.7.0 keeps its JSON-RPC method-name
// constants in an internal package, so they cannot be imported here; the names
// below are the re-exported copy of that surface, and TestServedMethodsMatchHandler
// fails if the handler ever stops dispatching one of them. A published Agent
// Card may only claim what this list actually serves.
const (
	MethodSendMessage          = "SendMessage"
	MethodSendStreamingMessage = "SendStreamingMessage"
	MethodGetTask              = "GetTask"
	MethodListTasks            = "ListTasks"
	MethodCancelTask           = "CancelTask"
	MethodSubscribeToTask      = "SubscribeToTask"
	MethodGetPushConfig        = "GetTaskPushNotificationConfig"
	MethodCreatePushConfig     = "CreateTaskPushNotificationConfig"
	MethodListPushConfigs      = "ListTaskPushNotificationConfigs"
	MethodDeletePushConfig     = "DeleteTaskPushNotificationConfig"
	MethodGetExtendedAgentCard = "GetExtendedAgentCard"
)

// ServedProtocolBinding / ServedProtocolVersion are the protocol claims a
// public interface in the Agent Card must make: exactly the binding and version
// the mounted handler speaks. a2a.Version is the version the transport stamps
// into every call context, so this stays true across library upgrades.
const (
	ServedProtocolBinding = string(a2a.TransportProtocolJSONRPC)
	ServedProtocolVersion = string(a2a.Version)
)

// ServedMethods is the complete non-streaming + streaming JSON-RPC method set
// answered on the public face. Anything else is a JSON-RPC METHOD_NOT_FOUND
// (-32601) refusal, which is the honest answer, never a 200 pretending success.
var ServedMethods = []string{
	MethodSendMessage, MethodSendStreamingMessage, MethodGetTask, MethodListTasks,
	MethodCancelTask, MethodSubscribeToTask,
	MethodGetPushConfig, MethodCreatePushConfig, MethodListPushConfigs, MethodDeletePushConfig,
	MethodGetExtendedAgentCard,
}

// UnservedLegacyMethods are the A2A v0.3 method names (message/send, tasks/get,
// ...). They belong to a2acompat/a2av0, a compatibility layer that lives in the
// separate a2a-go v0 module and is NOT part of this build, so this plane answers
// them with METHOD_NOT_FOUND. A card that names one of them advertises a method
// it does not serve -- a false claim about the deployment -- and is rejected at
// load time rather than at the client's first call.
var UnservedLegacyMethods = []string{
	"message/send", "message/stream",
	"tasks/get", "tasks/list", "tasks/cancel", "tasks/resubscribe",
	"tasks/pushNotificationConfig/set", "tasks/pushNotificationConfig/get",
	"tasks/pushNotificationConfig/list", "tasks/pushNotificationConfig/delete",
	"agent/getAuthenticatedExtendedCard",
}

// MethodIsServed reports whether name is dispatched by the mounted handler.
func MethodIsServed(name string) bool {
	for _, m := range ServedMethods {
		if m == name {
			return true
		}
	}
	return false
}

// unservedMethodClaims returns the v0 method names the card text advertises.
// The whole serialised document is scanned (names, descriptions, tags) because
// the claim does not have to live in a machine-readable field to mislead a
// client: the emil consumer resolves the card and calls the method it names.
func unservedMethodClaims(card *a2a.AgentCard) []string {
	if card == nil {
		return nil
	}
	var hits []string
	for _, m := range UnservedLegacyMethods {
		if strings.Contains(cardText(card), m) {
			hits = append(hits, m)
		}
	}
	return hits
}

// cardText serialises the card for text-level claims checks. Encoding failures
// yield an empty string: assertNoLeak already fails the load on a card that
// cannot be serialised, so the claims check never has to invent an error path.
func cardText(card *a2a.AgentCard) string {
	raw, err := json.Marshal(card)
	if err != nil {
		return ""
	}
	return string(raw)
}

// quote renders a claimed value in an error message unambiguously, so an empty
// or whitespace-only claim is visible as such in the load failure.
func quote(s string) string { return strconv.Quote(s) }
