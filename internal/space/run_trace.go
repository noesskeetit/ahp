package space

import (
	"go.opentelemetry.io/otel/attribute"
)

// RunTraceAttributes returns the identifier/measure-only span attributes the
// runner stamps on a turn's durable execution span (space.run). PDF rev3.2
// requires the whole task tree to be visible in traces while the collector never
// carries prompts, tool arguments or model output: "захват содержимого в
// OpenTelemetry выключен" (:347/:277). Every value below is a stable identifier
// that already exists in the durable lease, never request content, so the same
// function is what the coordinator stamps and what the byte-level content-free
// test asserts -- the wiring and its guard can never drift apart.
//
// Role is deliberately absent: at this hop there is no content-free source for a
// role label, and inventing one from request-adjacent data would risk the exact
// leak this function forbids (the model hop's refuseNamed omits role too).
func RunTraceAttributes(tenant, deployment, task, runID, threadID string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("ahp.component", "space"),
		attribute.String("ahp.operation", "run"),
		attribute.String("ahp.tenant", tenant),
		attribute.String("ahp.deployment", deployment),
		attribute.String("ahp.task", task),
		attribute.String("ahp.run", runID),
		attribute.String("ahp.thread", threadID),
	}
}
