//go:build !windows

package tools

import (
	"os"
	"syscall"
)

// openForReading opens a file read-only and nonblocking, so opening a FIFO
// does not wait for a writer; with noFollow, a symbolic link as the last
// component fails (O_NOFOLLOW).
func openForReading(name string, noFollow bool) (*os.File, error) {
	flags := os.O_RDONLY | syscall.O_NONBLOCK
	if noFollow {
		flags |= syscall.O_NOFOLLOW
	}
	return os.OpenFile(name, flags, 0)
}
