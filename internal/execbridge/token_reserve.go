package execbridge

import (
	"encoding/json"
	"github.com/ahp-platform/ahp/internal/model"
)

// Reserve input as UTF-8 bytes (a conservative bound for the configured
// byte-tokenizing providers), serialized roles/tools/framing, and the complete
// output cap. A deployment's fixed reserve is only a minimum, never a cap on
// prompt cost. Provider usage remains authoritative at settlement.
func tokenReserve(req model.ChatRequest, minimum int64) (int64, error) {
	encoded, err := json.Marshal(req)
	if err != nil {
		return 0, err
	}
	return max(minimum, int64(len(encoded))+int64(req.MaxTokens)), nil
}
