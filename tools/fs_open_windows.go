//go:build windows

package tools

import "os"

// openForReading opens a file read-only; Windows has no O_NOFOLLOW, so a
// symbolic link is checked before opening (not race-free).
func openForReading(name string, noFollow bool) (*os.File, error) {
	if noFollow {
		if info, err := os.Lstat(name); err == nil && info.Mode()&os.ModeSymlink != 0 {
			return nil, &symlinkRefusal{}
		}
	}
	return os.Open(name)
}
