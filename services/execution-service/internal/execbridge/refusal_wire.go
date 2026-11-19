package execbridge

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/ahp-platform/ahp/pkg/contracts"
)

// The Model Gateway names the refusing check in the response BODY
// (pkg/transport/json.go WriteError: {"code","message"} plus "reason" when the
// refusal has a stable domain reason). The status alone cannot distinguish
// "child must share tenant and subject" from "task ancestry changes owner",
// and it is the body that a reader in another process needs (DEF-EXECBRIDGE-REFUSAL-REASON-1).
const (
	// refusalBodyReadLimit bounds what a refusal response can feed into an
	// error, so an upstream blob can never size a durable frame.
	refusalBodyReadLimit = 8 << 10
	// refusalMessageRunes / refusalExcerptRunes bound the surfaced text.
	refusalMessageRunes = 512
	refusalExcerptRunes = 180
)

// refusalError renders a refused Model Gateway call as an error that keeps the
// HTTP status, the contract code as a CODE (contracts.Code branches on it
// instead of matching status text), and the published reason (contracts.ReasonOf)
// when there is one. Nothing here softens the refusal: the status is reported as
// the gateway sent it - 403 stays 403, 503 stays 503 - and the caller still gets
// an error. `prefix` keeps its historical wording so existing readers, the
// close-reason dictionary and the Dot harness keep matching.
//
// A body that is not a contract error - a proxy page, a truncated write -
// contributes only a bounded, whitespace-collapsed excerpt.
func refusalError(prefix string, status int, body io.Reader) error {
	data, _ := io.ReadAll(io.LimitReader(body, refusalBodyReadLimit))
	var wire struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Reason  string `json:"reason"`
	}
	_ = json.Unmarshal(data, &wire)
	if code := strings.TrimSpace(wire.Code); code != "" {
		// WriteError puts err.Error() in "message", which already begins with
		// "CODE: "; repeating it would bury the check that actually refused.
		message := strings.TrimPrefix(strings.TrimSpace(collapseWhitespace(wire.Message)), code+": ")
		if message == "" {
			message = fmt.Sprintf("gateway returned HTTP %d with no message", status)
		}
		cause := contracts.NewError(code, truncateRunes(message, refusalMessageRunes))
		if reason := strings.TrimSpace(wire.Reason); reason != "" {
			cause = contracts.NewReasoned(code, reason, truncateRunes(message, refusalMessageRunes))
		}
		return fmt.Errorf("%s: HTTP %d: %w", prefix, status, cause)
	}
	if excerpt := collapseWhitespace(string(data)); excerpt != "" {
		return fmt.Errorf("%s: HTTP %d: %s", prefix, status, truncateRunes(excerpt, refusalExcerptRunes))
	}
	return fmt.Errorf("%s: HTTP %d", prefix, status)
}

// collapseWhitespace makes an upstream body safe to put on one error line: no
// control characters, no newlines, runs of spaces folded.
func collapseWhitespace(s string) string {
	return strings.Join(strings.Fields(strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, s)), " ")
}

func truncateRunes(s string, limit int) string {
	runes := []rune(s)
	if len(runes) <= limit {
		return s
	}
	return string(runes[:limit]) + "…(truncated)"
}
