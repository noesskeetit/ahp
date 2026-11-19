//go:build linux

package compute

import (
	"os"
	"syscall"
)

// ioctl numbers for the two kernel objects a microVM cannot exist without.
//
// They are _IO() numbers in the kernel uapi, i.e. direction and size bits are
// ZERO and the argument travels in the third ioctl argument:
//
//	#define KVM_CREATE_VM   _IO(KVMIO, 0x01)   // linux/kvm.h, returns a VM fd
//	#define KVM_CREATE_VCPU _IO(KVMIO, 0x41)   // linux/kvm.h, on the VM fd
//
// with KVMIO = 0xAE. So the numbers are (0xAE<<8)|nr, nothing else. Getting
// this wrong is not a harmless typo: an ioctl number the kernel does not map is
// answered with EINVAL by the very same default case that a genuine
// "you cannot boot a guest here" answer uses, so a wrong constant makes a
// working KVM host read as broken. That is how this file first shipped (it used
// 0xAE40, which is the vCPU-adjacent range and unmapped), and it is why
// TestKVMIoctlNumbersMatchKernelUAPI pins both constants to the header text.
const (
	kvmIOCTLType       = 0xAE
	kvmCreateVM        = uintptr(kvmIOCTLType<<8 | 0x01)
	kvmCreateVCPU      = uintptr(kvmIOCTLType<<8 | 0x41)
	kvmCreateVMRequest = "KVM_CREATE_VM"
)

// kvmVMCreateProblem asks the kernel for the two objects a microVM cannot exist
// without: a VM, then a vCPU for it. It returns "" only when the kernel hands
// both over, and names the failing request otherwise.
//
// Why openability is not enough: a node can exist, open read-write and answer
// KVM_CHECK_EXTENSION with confident numbers while the kernel still refuses the
// objects themselves, because hardware virtualisation is not actually usable
// for this guest. Trusting the node instead of the kernel is what turns a class-X
// isolation claim into a false one, so the gate asks for the objects.
//
// A permission refusal never reaches here: kvmProblem() already named it, and
// this function stays silent in that case so the caller reports the sharper
// reason.
func kvmVMCreateProblem() string {
	f, err := os.OpenFile(kvmDevicePath, os.O_RDWR, 0)
	if err != nil {
		return ""
	}
	defer func() { _ = f.Close() }()

	vm, _, errno := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), kvmCreateVM, 0)
	if errno != 0 {
		return "KVM device opens but the kernel refuses to create a VM on it: " + kvmCreateVMRequest +
			" failed with " + syscall.Errno(errno).Error() +
			"; this host cannot boot a guest even though the node answers capability queries" +
			" (a nested guest whose hypervisor does not let it use VMX/SVM controls is refused here);" +
			" this is not a permission problem"
	}
	vmFD := int(vm)
	defer func() { _ = syscall.Close(vmFD) }()

	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(vmFD), kvmCreateVCPU, 0); errno != 0 {
		return "a VM can be created on this KVM node but the kernel refuses a vCPU for it: KVM_CREATE_VCPU" +
			" failed with " + syscall.Errno(errno).Error() +
			"; a guest without a vCPU runs nothing, so this host is not usable for class X"
	}
	return ""
}
