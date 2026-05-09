//go:build windows

package simconnect

import (
	"syscall"
	"unsafe"

)

func stringToBytePtr(name string) (*byte, error) {
	return syscall.BytePtrFromString(name)
}

func isHRESULTSuccess(hresult uintptr) bool {
	// COM success: high bit clear. S_OK (0) and S_FALSE (1) are both success.
	return !isHRESULTFailure(hresult)
}

func isHRESULTFailure(hresult uintptr) bool {
	// HRESULT failure is indicated by the high bit being set (0x80000000)
	return (uint32(hresult) & 0x80000000) != 0
}

func toUnsafePointer[T any](ptr *T) uintptr {
	return uintptr(unsafe.Pointer(ptr))
}
