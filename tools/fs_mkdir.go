package tools

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

// mkdirAll creates a directory and its missing parents as Node's recursive
// mkdir does (MKDirpSync): it creates the path first and goes up only for a
// missing parent, so an error names the call that failed; a file in the
// way is EEXIST at the path and ENOTDIR above it.
func mkdirAll(path string) error { return mkdirp(path, false) }

func mkdirp(path string, below bool) error {
	err := os.Mkdir(path, 0o777)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, syscall.EACCES), errors.Is(err, syscall.ENOSPC), errors.Is(err, syscall.ENOTDIR), errors.Is(err, syscall.EPERM):
		return nodeError(err)
	case errors.Is(err, syscall.ENOENT):
		parent := filepath.Dir(path)
		if parent == path {
			return syscallError("mkdir", path, syscall.EEXIST)
		}
		if err := mkdirp(parent, true); err != nil {
			return err
		}
		if err := os.Mkdir(path, 0o777); err != nil && !isDir(path) {
			return nodeError(err)
		}
		return nil
	}
	info, statErr := os.Stat(path)
	if statErr != nil {
		return nodeError(statErr)
	}
	if !info.IsDir() {
		if below && errors.Is(err, syscall.EEXIST) {
			return syscallError("mkdir", path, syscall.ENOTDIR)
		}
		return syscallError("mkdir", path, syscall.EEXIST)
	}
	return nil
}

func isDir(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}
