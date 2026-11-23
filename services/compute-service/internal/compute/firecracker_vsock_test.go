package compute

import (
	"context"
	"path/filepath"
	"testing"
)

func TestConfigureFirecrackerVsockUsesGuestCIDAndHostUnixBase(t *testing.T) {
	dir := shortSandboxDir(t)
	fake := newFakeFirecracker(t, filepath.Join(dir, "api.sock"))
	base := filepath.Join(dir, "vsock.sock")
	if err := newFCClient(fake.socket).putVSock(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	calls := fake.snapshot()
	if len(calls) != 1 || calls[0].Method != "PUT" || calls[0].Path != "/vsock" ||
		calls[0].Body["guest_cid"] != float64(3) || calls[0].Body["uds_path"] != base {
		t.Fatalf("unexpected Firecracker API calls %+v", calls)
	}
}
