//go:build darwin

package atrestverify_test

import (
	"context"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// sysLoadAvg records the machine load the measurement legs ran under. Ambient load is part of an
// overhead number, so it is written next to the numbers instead of being assumed away.
// vm.loadavg prints "{ 1.00 2.00 3.00 }".
func sysLoadAvg(out []float64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	raw, err := exec.CommandContext(ctx, "sysctl", "-n", "vm.loadavg").Output()
	if err != nil {
		return err
	}
	fields := strings.Fields(strings.Trim(strings.TrimSpace(string(raw)), "{}"))
	for i := 0; i < len(out) && i < len(fields); i++ {
		v, err := strconv.ParseFloat(fields[i], 64)
		if err != nil {
			return err
		}
		out[i] = v
	}
	return nil
}
