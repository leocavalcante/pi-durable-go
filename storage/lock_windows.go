//go:build windows

package storage

import (
	"errors"
	"os"
)

func lockFile(_ *os.File) (func() error, error) {
	return nil, errors.New("storage: JSONL advisory locking is unsupported on Windows")
}
