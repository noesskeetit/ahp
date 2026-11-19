package execbridge

import "context"

// FenceTenant delegates tenant admission and cancellation to the Model owner.
func (b *Bridge) FenceTenant(ctx context.Context, tenant string) error {
	return b.svc.FenceTenant(ctx, tenant)
}

// DeleteTenant delegates final erasure after provider reconciliation.
func (b *Bridge) DeleteTenant(ctx context.Context, tenant string) error {
	return b.svc.DeleteTenant(ctx, tenant)
}
