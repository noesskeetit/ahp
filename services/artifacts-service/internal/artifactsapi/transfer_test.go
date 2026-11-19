package artifactsapi

import (
	"net/http"
	"testing"
)

func TestTransferRoutesFailClosedWithoutAuthority(t *testing.T) {
	e := newEnv(t)
	token := e.signTask(t, alice)
	for _, op := range []string{"TransferGrant", "CopyFromTransferGrant"} {
		status, body := e.call(t, op, token, `{}`)
		mustStatus(t, status, http.StatusForbidden, body)
	}
}
