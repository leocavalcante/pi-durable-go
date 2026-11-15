//go:build !linux

package tools

import "errors"

// nativeWatching: elsewhere the local environment polls. Upstream watches
// natively on macOS with FSEvents; this port has no native watcher there.
const nativeWatching = false

type nativeWatcher struct{}

func newNativeWatcher() (*nativeWatcher, error) {
	return nil, errors.New("native watching is not supported")
}

func (w *nativeWatcher) add(string, func(string)) (func(), error) {
	return nil, errors.New("native watching is not supported")
}

func (w *nativeWatcher) close() {}

func anyUnreliable([]string) bool { return false }
