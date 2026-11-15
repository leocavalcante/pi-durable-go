//go:build linux

package tools

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"unsafe"

	"golang.org/x/sys/unix"
)

// nativeWatching: Linux watches natively, with inotify, as libuv's fs.watch.
const nativeWatching = true

// inotifyMask is libuv's: what fs.watch reports.
const inotifyMask = unix.IN_ATTRIB | unix.IN_CREATE | unix.IN_MODIFY | unix.IN_DELETE | unix.IN_DELETE_SELF |
	unix.IN_MOVE_SELF | unix.IN_MOVED_FROM | unix.IN_MOVED_TO

// nativeWatcher is one inotify instance; each added path calls back with
// the name an event is about, or its own base name for events on itself,
// as libuv reports them.
type nativeWatcher struct {
	file *os.File
	fd   int
	mu   sync.Mutex
	byWD map[int32][]*nativeWatch
}

type nativeWatch struct {
	path     string
	callback func(name string)
}

func newNativeWatcher() (*nativeWatcher, error) {
	fd, err := unix.InotifyInit1(unix.IN_CLOEXEC | unix.IN_NONBLOCK)
	if err != nil {
		return nil, err
	}
	w := &nativeWatcher{file: os.NewFile(uintptr(fd), "inotify"), fd: fd, byWD: map[int32][]*nativeWatch{}}
	go w.read()
	return w, nil
}

func (w *nativeWatcher) add(path string, callback func(name string)) (func(), error) {
	wd, err := unix.InotifyAddWatch(w.fd, path, inotifyMask)
	if err != nil {
		return nil, &os.PathError{Op: "watch", Path: path, Err: err}
	}
	watch := &nativeWatch{path: path, callback: callback}
	w.mu.Lock()
	w.byWD[int32(wd)] = append(w.byWD[int32(wd)], watch)
	w.mu.Unlock()
	return func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		list := w.byWD[int32(wd)]
		for i, other := range list {
			if other == watch {
				list = append(list[:i:i], list[i+1:]...)
				break
			}
		}
		if len(list) == 0 {
			delete(w.byWD, int32(wd))
			_, _ = unix.InotifyRmWatch(w.fd, uint32(wd))
		} else {
			w.byWD[int32(wd)] = list
		}
	}, nil
}

func (w *nativeWatcher) close() { _ = w.file.Close() }

func (w *nativeWatcher) read() {
	buf := make([]byte, 64*1024)
	for {
		n, err := w.file.Read(buf)
		if err != nil {
			if errors.Is(err, os.ErrClosed) {
				return
			}
			continue
		}
		for offset := 0; offset+unix.SizeofInotifyEvent <= n; {
			event := (*unix.InotifyEvent)(unsafe.Pointer(&buf[offset]))
			nameBytes := buf[offset+unix.SizeofInotifyEvent : offset+unix.SizeofInotifyEvent+int(event.Len)]
			offset += unix.SizeofInotifyEvent + int(event.Len)
			// Queue overflows and removed watches report nothing, as in
			// libuv.
			if event.Wd == -1 || event.Mask&unix.IN_IGNORED != 0 {
				continue
			}
			name := string(nameBytes)
			for len(name) > 0 && name[len(name)-1] == 0 {
				name = name[:len(name)-1]
			}
			w.mu.Lock()
			watches := append([]*nativeWatch(nil), w.byWD[event.Wd]...)
			w.mu.Unlock()
			for _, watch := range watches {
				reported := name
				if reported == "" {
					reported = filepath.Base(watch.path)
				}
				watch.callback(reported)
			}
		}
	}
}

// unreliableFileSystems are Linux statfs magic numbers of file systems that
// accept watches but do not report changes made elsewhere.
var unreliableFileSystems = map[int64]bool{
	0x6969: true, 0x517b: true, 0xff534d42: true, 0xfe534d42: true, 0x65735546: true, 0x01021997: true,
	0x0bd00bd0: true, 0x47504653: true, 0x00c36400: true, 0x5346414f: true, 0x6b414653: true, 0x5dca2df5: true,
}

// anyUnreliable reports whether any path, or its nearest existing ancestor,
// is on a file system that does not report changes made elsewhere.
func anyUnreliable(paths []string) bool {
	for _, path := range paths {
		for _, candidate := range append([]string{path}, ancestorsOf(path)...) {
			var stat unix.Statfs_t
			if unix.Statfs(candidate, &stat) != nil {
				continue
			}
			if unreliableFileSystems[int64(uint32(stat.Type))] {
				return true
			}
			break
		}
	}
	return false
}
