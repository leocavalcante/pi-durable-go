//go:build !windows

package tools

import (
	"os"
	"syscall"
)

// rename renames with rename(2)'s own errors: os.Rename reports a directory
// destination as EEXIST where the system and Node report EISDIR.
func rename(from, to string) error {
	if err := syscall.Rename(from, to); err != nil {
		return &os.LinkError{Op: "rename", Old: from, New: to, Err: err}
	}
	return nil
}
