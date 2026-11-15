//go:build !windows

package tools

import (
	"errors"
	"io/fs"
	"syscall"
)

// fileIdentity is a file's device and inode.
func fileIdentity(info fs.FileInfo) (uint64, uint64) {
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		return uint64(stat.Dev), uint64(stat.Ino)
	}
	return 0, 0
}

func isNotDirectory(err error) bool { return errors.Is(err, syscall.ENOTDIR) }
