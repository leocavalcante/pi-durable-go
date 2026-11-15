//go:build windows

package tools

import (
	"errors"
	"io/fs"
	"syscall"
)

// fileIdentity: Windows has no inode in FileInfo; identity is not compared.
func fileIdentity(fs.FileInfo) (uint64, uint64) { return 0, 0 }

func isNotDirectory(err error) bool { return errors.Is(err, syscall.ENOTDIR) }
