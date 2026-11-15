//go:build windows

package tools

import "os"

func rename(from, to string) error { return os.Rename(from, to) }
