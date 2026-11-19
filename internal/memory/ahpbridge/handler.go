package ahpbridge

import (
	"net/http"

	"github.com/ahp-platform/ahp/internal/memory/longterm"
	"github.com/ahp-platform/ahp/internal/memory/longterm/httpapi"
	"github.com/ahp-platform/ahp/pkg/auth"
	"github.com/ahp-platform/ahp/pkg/contracts"
	"github.com/ahp-platform/ahp/pkg/transport"
)

func NewHandler(service *memory.Service, verifier *auth.Verifier, authorize transport.Authorize) (http.Handler, error) {
	if service == nil || verifier == nil || authorize == nil {
		return nil, contracts.NewError(contracts.ErrInvalid, "Hindsight memory needs the existing AHP verifier and live authority")
	}
	return transport.Task(verifier, "ahp", "memory", authorize)(withRPC(httpapi.New(service))), nil
}
