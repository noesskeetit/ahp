package state

import "context"

type CommonRootRequest struct {
	Namespace       string `json:"namespace"`
	Key             string `json:"key"`
	OperationKey    string `json:"operation_key"`
	ExpectedVersion int64  `json:"expected_version"`
	Revision        *int64 `json:"revision"`
}
type CommonRoot struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Version   int64  `json:"version"`
	Revision  *int64 `json:"revision"`
}

// A nil revision releases a pin but keeps its version and replay receipt.
func (c *Client) PutCommonRoot(ctx context.Context, f Fence, root CommonRootRequest) (CommonRoot, error) {
	var out CommonRoot
	if e := validateFence(f); e != nil {
		return out, e
	}
	if root.Key == "" || root.OperationKey == "" || root.ExpectedVersion < 0 || root.Revision != nil && *root.Revision < 1 {
		return out, newInvalid("invalid common root")
	}
	e := c.callTask(ctx, "PutCommonRoot", struct {
		Fence Fence             `json:"fence"`
		Root  CommonRootRequest `json:"root"`
	}{f, root}, &out)
	return out, e
}
