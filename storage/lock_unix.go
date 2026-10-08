//go:build !windows

package storage

import (
	"os"
	"syscall"
)

func lockFile(f *os.File) (func() error, error) {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return nil, err
	}
	return func() error { return syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }, nil
}
