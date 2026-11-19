//go:build linux

package compute

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"
)

// A wrong ioctl number is indistinguishable, at the syscall boundary, from the
// refusal this gate exists to detect: an unmapped request number on the KVM fd
// is answered EINVAL, and so is a host whose hypervisor will not let it build a
// guest. This test removes the guesswork by reading the kernel uapi header this
// machine actually has and checking the two _IO() numbers against it.
//
// It is a correctness check on the constants, not on the environment: on a
// machine with no installed header it says so and stops, it never asserts that
// KVM works.
func TestKVMIoctlNumbersMatchKernelUAPI(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the KVM ioctl numbers come from the Linux uapi header")
	}
	header, ok := findKVMUAPIHeader()
	if !ok {
		t.Skip("no /usr/include/linux/kvm.h on this machine; numbers not cross-checked here")
	}
	raw, err := os.ReadFile(header)
	if err != nil {
		t.Fatalf("read %s: %v", header, err)
	}
	t.Logf("cross-checked against %s", header)

	for _, want := range []struct {
		name string
		nr   int
		have uintptr
	}{
		{"KVM_CREATE_VM", 0x01, kvmCreateVM},
		{"KVM_CREATE_VCPU", 0x41, kvmCreateVCPU},
	} {
		nr, direction, found := readIODefine(string(raw), want.name)
		if !found {
			t.Fatalf("%s: no #define found in %s", want.name, header)
		}
		if direction != "O" {
			t.Fatalf("%s: header uses _%s(), the gate assumes the argument-less _IO() form; fix the constant, not the test", want.name, direction)
		}
		if nr != want.nr {
			t.Fatalf("%s: header says nr 0x%02x, the gate uses 0x%02x", want.name, nr, want.nr)
		}
		if got := uintptr(kvmIOCTLType<<8 | uintptr(nr)); got != want.have {
			t.Fatalf("%s: computed _IO(0x%x,0x%02x) = 0x%x, the gate uses 0x%x", want.name, kvmIOCTLType, nr, got, want.have)
		}
		t.Logf("%s = _I%s(0x%x, 0x%02x) = 0x%x - gate matches", want.name, direction, kvmIOCTLType, nr, want.have)
	}
}

// findKVMUAPIHeader returns the first installed copy of the KVM uapi header.
func findKVMUAPIHeader() (string, bool) {
	candidates := []string{"/usr/include/linux/kvm.h"}
	if matches, _ := filepath.Glob("/usr/src/linux-headers/*/include/uapi/linux/kvm.h"); len(matches) > 0 {
		candidates = append(candidates, matches[0])
	}
	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			return path, true
		}
	}
	return "", false
}

// readIODefine reads one `#define NAME _IO(KVMIO, nr)` line and reports the
// ioctl number, the _IO family letter (IO/IOR/IOW/IOWR) and whether it was found.
func readIODefine(source, name string) (nr int, direction string, found bool) {
	pattern := `#define\s+` + regexp.QuoteMeta(name) + `\s+_I(O|OR|OW|OWR)\(\s*KVMIO\s*,\s*0x([0-9a-fA-F]+)`
	re, err := regexp.Compile(pattern)
	if err != nil {
		return 0, "", false
	}
	match := re.FindStringSubmatch(source)
	if match == nil {
		return 0, "", false
	}
	parsed := 0
	if _, err := fmt.Sscanf(match[2], "%x", &parsed); err != nil {
		return 0, "", false
	}
	return parsed, match[1], true
}
