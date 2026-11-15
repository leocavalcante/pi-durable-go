package tools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf16"
)

// Watching files, ported from upstream's NodeFileWatcher (env/node-watch.ts):
// watches by snapshots, so native events only trigger a debounced rescan, and
// changes are the difference between snapshots plus the event paths.

// WatchTarget is a file or directory to watch (upstream WatchTarget); it may
// be missing, and creating it is a change.
type WatchTarget struct {
	Path string
	// Recursive watches everything below a directory, not only its entries;
	// symbolic links below it are not followed.
	Recursive bool
	// ExcludeHidden and ExcludeNames are entries below Path that are neither
	// watched nor reported: names starting with ".", or these names.
	ExcludeHidden bool
	ExcludeNames  []string
}

// FileChange is what changed (upstream FileChange), by Kind:
//   - "paths": something at or below each of Paths may have changed (a
//     directory path covers its subtree); calls may be spurious, and no
//     change is missed while the watcher is healthy;
//   - "overflow": coverage was uncertain for a while; rescan everything
//     watched;
//   - "failed": the watcher stopped with Err; no calls follow.
type FileChange struct {
	Kind  string
	Paths []string
	Err   *FileError
}

// WatchOptions are how a local environment watches (upstream
// NodeWatchOptions).
type WatchOptions struct {
	// Mode forces "native" or "polling". By default Linux watches natively
	// unless a file system does not report changes made elsewhere (network
	// and FUSE file systems), and other systems poll.
	Mode string
	// PollInterval is the time between snapshots in polling mode; default
	// 2 s.
	PollInterval time.Duration
	// MaxDirectories is the most directories one watcher covers; default
	// 10,000.
	MaxDirectories int
}

const (
	watchDebounce          = 50 * time.Millisecond
	defaultPollInterval    = 2 * time.Second
	defaultMaxDirectories  = 10000
	watchHashMaxBytes      = 256 * 1024
	watchHashRecent        = 5 * time.Second
	maxSyncRounds          = 10
	errWatchBudgetExceeded = "Watched paths exceed %d directories"
)

// Watch reports changes to files and directories (upstream FileSystem.watch).
// When it returns, coverage is established: a host that watches before it
// loads cannot miss a change made during the load. onChange is called from
// one goroutine at a time; a panic in it does not stop the watching.
func (e *LocalEnv) Watch(ctx context.Context, targets []WatchTarget, onChange func(FileChange)) (*FileWatcher, error) {
	if err := aborted(ctx, ""); err != nil {
		return nil, err
	}
	options := e.options.Watch
	resolved := make([]watchTarget, len(targets))
	for i, target := range targets {
		names := map[string]bool{}
		for _, name := range target.ExcludeNames {
			names[name] = true
		}
		resolved[i] = watchTarget{path: e.resolve(target.Path), recursive: target.Recursive, hidden: target.ExcludeHidden, names: names}
	}
	mode := options.Mode
	if mode == "" {
		mode = "native"
		paths := make([]string, len(resolved))
		for i, target := range resolved {
			paths[i] = target.path
		}
		if !nativeWatching || anyUnreliable(paths) {
			mode = "polling"
		}
	}
	w := &FileWatcher{targets: resolved, onChange: onChange, mode: mode, watchers: map[string]installedWatch{}, events: map[string]bool{},
		pollInterval: options.PollInterval, maxDirectories: options.MaxDirectories}
	if w.pollInterval <= 0 {
		w.pollInterval = defaultPollInterval
	}
	if w.maxDirectories <= 0 {
		w.maxDirectories = defaultMaxDirectories
	}
	if _, err := w.sync(false); err != nil {
		w.stopNow()
		var budget *budgetExceeded
		if errors.As(err, &budget) {
			return nil, &FileError{Code: "invalid", Message: budget.Error()}
		}
		return nil, nodeError(err)
	}
	w.schedulePoll()
	if err := aborted(ctx, ""); err != nil {
		w.Stop(context.Background())
		return nil, err
	}
	return w, nil
}

type watchTarget struct {
	path      string
	recursive bool
	hidden    bool
	names     map[string]bool
}

func (t watchTarget) excluded(name string) bool {
	return t.hidden && strings.HasPrefix(name, ".") || t.names[name]
}

// watchEntry is what a snapshot remembers of one path; directories and
// ancestors are compared by identity only.
type watchEntry struct {
	kind     string
	dev, ino uint64
	size     int64
	mtimeMs  float64
	hash     string
}

type installedWatch struct {
	remove   func()
	dev, ino uint64
}

type budgetExceeded struct{ limit int }

func (b *budgetExceeded) Error() string { return fmt.Sprintf(errWatchBudgetExceeded, b.limit) }

// FileWatcher watches files (upstream FileWatcher).
type FileWatcher struct {
	targets        []watchTarget
	onChange       func(FileChange)
	pollInterval   time.Duration
	maxDirectories int

	mu       sync.Mutex
	mode     string
	native   *nativeWatcher
	watchers map[string]installedWatch
	events   map[string]bool
	snapshot map[string]watchEntry
	timer    *time.Timer
	running  chan struct{} // closed when the flush in flight ends
	dirty    bool
	stopped  bool
	deliver  sync.Mutex // one onChange call at a time
}

// Mode is "native" when changes are reported within about two seconds, or
// "polling" when snapshots are compared, so a change undone between two of
// them can be missed.
func (w *FileWatcher) Mode() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.mode
}

// Stop stops watching; no onChange call starts after it returns.
func (w *FileWatcher) Stop(ctx context.Context) error {
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return nil
	}
	w.mu.Unlock()
	w.stopNow()
	w.mu.Lock()
	running := w.running
	w.mu.Unlock()
	if running != nil {
		select {
		case <-running:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	// A delivery in progress finishes before Stop returns.
	w.deliver.Lock()
	w.deliver.Unlock()
	return nil
}

func (w *FileWatcher) stopNow() {
	w.mu.Lock()
	w.stopped = true
	if w.timer != nil {
		w.timer.Stop()
		w.timer = nil
	}
	watchers := w.watchers
	w.watchers = map[string]installedWatch{}
	native := w.native
	w.native = nil
	w.mu.Unlock()
	for _, installed := range watchers {
		installed.remove()
	}
	if native != nil {
		native.close()
	}
}

func (w *FileWatcher) send(change FileChange) {
	w.deliver.Lock()
	defer w.deliver.Unlock()
	w.mu.Lock()
	stopped := w.stopped
	w.mu.Unlock()
	if stopped {
		return
	}
	// A panicking callback must not stop watching.
	func() {
		defer func() { _ = recover() }()
		w.onChange(change)
	}()
}

func (w *FileWatcher) schedulePoll() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped || w.mode != "polling" {
		return
	}
	w.timer = time.AfterFunc(w.pollInterval, func() {
		w.mu.Lock()
		w.timer = nil
		w.mu.Unlock()
		w.flush()
		w.schedulePoll()
	})
}

// scheduleFlush rescans after the debounce; mu is held.
func (w *FileWatcher) scheduleFlush() {
	if w.stopped || w.timer != nil {
		return
	}
	w.timer = time.AfterFunc(watchDebounce, func() {
		w.mu.Lock()
		w.timer = nil
		w.mu.Unlock()
		w.flush()
	})
}

// flush rescans and reports what changed; a flush asked for while one runs
// runs again after it.
func (w *FileWatcher) flush() {
	w.mu.Lock()
	if w.running != nil {
		w.dirty = true
		running := w.running
		w.mu.Unlock()
		<-running
		return
	}
	running := make(chan struct{})
	w.running = running
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		w.running = nil
		w.mu.Unlock()
		close(running)
	}()
	for {
		w.mu.Lock()
		w.dirty = false
		var events []string
		for path := range w.events {
			events = append(events, path)
		}
		w.events = map[string]bool{}
		w.mu.Unlock()
		changed, err := w.sync(true)
		if err != nil {
			var fileError *FileError
			if !errors.As(err, &fileError) {
				code := "invalid"
				if errors.Is(err, fs.ErrPermission) {
					code = "permission_denied"
				}
				fileError = &FileError{Code: code, Message: nodeError(err).Error(), Err: err}
			}
			w.send(FileChange{Kind: "failed", Err: fileError})
			w.stopNow()
			return
		}
		for _, path := range events {
			changed[path] = true
		}
		if len(changed) > 0 {
			paths := make([]string, 0, len(changed))
			for path := range changed {
				paths = append(paths, path)
			}
			sortUTF16(paths)
			w.send(FileChange{Kind: "paths", Paths: paths})
		}
		w.mu.Lock()
		again := w.dirty && !w.stopped
		w.mu.Unlock()
		if !again {
			return
		}
	}
}

// sortUTF16 sorts as JavaScript's default sort does: by UTF-16 code units.
func sortUTF16(values []string) {
	sort.Slice(values, func(i, j int) bool {
		a, b := utf16.Encode([]rune(values[i])), utf16.Encode([]rune(values[j]))
		for k := 0; k < len(a) && k < len(b); k++ {
			if a[k] != b[k] {
				return a[k] < b[k]
			}
		}
		return len(a) < len(b)
	})
}

// sync rescans, reports differences, and installs watchers for new
// directories, rescanning until none are new.
func (w *FileWatcher) sync(report bool) (map[string]bool, error) {
	changed := map[string]bool{}
	for round := 0; round < maxSyncRounds; round++ {
		w.mu.Lock()
		stopped := w.stopped
		w.mu.Unlock()
		if stopped {
			break
		}
		snapshot, linked, err := w.scan()
		if err != nil {
			return nil, err
		}
		w.mu.Lock()
		if w.stopped {
			w.mu.Unlock()
			break
		}
		if report {
			for _, path := range w.diff(w.snapshot, snapshot) {
				changed[path] = true
			}
		}
		w.snapshot = snapshot
		polling := w.mode == "polling"
		w.mu.Unlock()
		if polling || !w.reconcileWatchers(snapshot, linked) {
			break
		}
		// Something written into a new directory before its watcher existed
		// shows up in the next round.
		report = true
	}
	return changed, nil
}

func (w *FileWatcher) diff(previous, next map[string]watchEntry) []string {
	var changed []string
	for path, entry := range next {
		if before, ok := previous[path]; !ok || before != entry {
			changed = append(changed, w.reported(path))
		}
	}
	for path := range previous {
		if _, ok := next[path]; !ok {
			changed = append(changed, w.reported(path))
		}
	}
	return changed
}

// reported is the path to report for a change at path: an ancestor that
// changed identity moved every target below it.
func (w *FileWatcher) reported(path string) string {
	for _, target := range w.targets {
		if isWithin(path, target.path) {
			return path
		}
	}
	for _, target := range w.targets {
		if isWithin(target.path, path) {
			return target.path
		}
	}
	return path
}

func isWithin(path, ancestor string) bool {
	if path == ancestor {
		return true
	}
	if !strings.HasSuffix(ancestor, string(filepath.Separator)) {
		ancestor += string(filepath.Separator)
	}
	return strings.HasPrefix(path, ancestor)
}

// ancestorsOf lists path's ancestors, nearest first, up to the root.
func ancestorsOf(path string) []string {
	var out []string
	for current := filepath.Dir(path); ; current = filepath.Dir(current) {
		out = append(out, current)
		if filepath.Dir(current) == current {
			return out
		}
	}
}

func entryOf(info fs.FileInfo, hash string) watchEntry {
	kind := "other"
	switch mode := info.Mode(); {
	case mode.IsRegular():
		kind = "file"
	case mode.IsDir():
		kind = "directory"
	case mode&fs.ModeSymlink != 0:
		kind = "symlink"
	}
	dev, ino := fileIdentity(info)
	entry := watchEntry{kind: kind, dev: dev, ino: ino, hash: hash}
	if kind != "directory" {
		modified := info.ModTime()
		entry.size = info.Size()
		entry.mtimeMs = float64(modified.Unix())*1000 + float64(modified.Nanosecond())/1e6
	}
	return entry
}

func isDenied(err error) bool { return errors.Is(err, fs.ErrPermission) }

func (w *FileWatcher) scan() (map[string]watchEntry, map[string]bool, error) {
	w.mu.Lock()
	polling := w.mode == "polling"
	w.mu.Unlock()
	snapshot := map[string]watchEntry{}
	linked := map[string]bool{}
	counted := map[string]bool{}
	traversed := map[string]bool{}
	listed := map[string]string{}
	countDirectory := func(path string) error {
		counted[path] = true
		if len(counted) > w.maxDirectories {
			return &budgetExceeded{limit: w.maxDirectories}
		}
		return nil
	}
	record := func(path string, info fs.FileInfo) {
		hash := ""
		if polling && info.Mode().IsRegular() && info.Size() <= watchHashMaxBytes && time.Since(info.ModTime()) < watchHashRecent {
			if content, err := os.ReadFile(path); err == nil {
				sum := sha256.Sum256(content)
				hash = hex.EncodeToString(sum[:])
			}
		}
		snapshot[path] = entryOf(info, hash)
	}
	var scanDirectory func(index int, target watchTarget, directory string) error
	scanDirectory = func(index int, target watchTarget, directory string) error {
		key := fmt.Sprintf("%d\x00%s", index, directory)
		if traversed[key] {
			return nil
		}
		traversed[key] = true
		dir, err := os.Open(directory)
		var names []string
		if err == nil {
			names, err = dir.Readdirnames(-1)
			dir.Close()
		}
		if err != nil {
			// The watched directory itself must be readable; below it,
			// unreadable directories are skipped.
			if directory == target.path && isDenied(err) {
				return err
			}
			if errors.Is(err, fs.ErrNotExist) || isDenied(err) || isNotDirectory(err) {
				return nil
			}
			return err
		}
		for _, name := range names {
			if target.excluded(name) {
				continue
			}
			path := directory
			if !strings.HasSuffix(path, string(filepath.Separator)) {
				path += string(filepath.Separator)
			}
			path += name
			kind, ok := listed[path]
			if !ok {
				info, err := os.Lstat(path)
				if err != nil {
					continue
				}
				kind = entryOf(info, "").kind
				listed[path] = kind
				// A target's own entry (following links) wins over its
				// listing by another target.
				if _, recorded := snapshot[path]; !recorded {
					record(path, info)
				}
			}
			if target.recursive && kind == "directory" {
				if err := countDirectory(path); err != nil {
					return err
				}
				if err := scanDirectory(index, target, path); err != nil {
					return err
				}
			}
		}
		return nil
	}
	for index, target := range w.targets {
		for _, ancestor := range ancestorsOf(target.path) {
			if _, ok := snapshot[ancestor]; ok {
				continue
			}
			// Identity only: an ancestor's own times change with every
			// unrelated sibling.
			if info, err := os.Lstat(ancestor); err == nil {
				entry := entryOf(info, "")
				entry.size, entry.mtimeMs = 0, 0
				snapshot[ancestor] = entry
			}
		}
		// The target may be a symbolic link to what is watched; follow it. A
		// missing target is watched for its creation; one that cannot be
		// reached for lack of permission fails.
		info, err := os.Stat(target.path)
		if err != nil {
			if isDenied(err) {
				return nil, nil, err
			}
			continue
		}
		record(target.path, info)
		if info.Mode().IsRegular() {
			if own, err := os.Lstat(target.path); err == nil && own.Mode()&fs.ModeSymlink != 0 {
				linked[target.path] = true
			}
		}
		if info.IsDir() {
			if err := countDirectory(target.path); err != nil {
				return nil, nil, err
			}
			if err := scanDirectory(index, target, target.path); err != nil {
				return nil, nil, err
			}
		}
	}
	return snapshot, linked, nil
}

// reconcileWatchers watches every existing ancestor of each target, each
// target directory, each target that is a symbolic link to a file, and each
// directory below a recursive target; it reports whether one was added.
func (w *FileWatcher) reconcileWatchers(snapshot map[string]watchEntry, linked map[string]bool) bool {
	wanted := map[string]bool{}
	for path := range linked {
		wanted[path] = true
	}
	for _, target := range w.targets {
		for _, ancestor := range ancestorsOf(target.path) {
			if snapshot[ancestor].kind == "directory" {
				wanted[ancestor] = true
			}
		}
		if snapshot[target.path].kind != "directory" {
			continue
		}
		wanted[target.path] = true
		if target.recursive {
			for path, entry := range snapshot {
				if entry.kind == "directory" && path != target.path && isWithin(path, target.path) {
					wanted[path] = true
				}
			}
		}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		return false
	}
	for path, installed := range w.watchers {
		entry, ok := snapshot[path]
		// Gone, or replaced: a watcher follows the directory it was
		// installed on, not the path.
		if !wanted[path] || !ok || entry.dev != installed.dev || entry.ino != installed.ino {
			installed.remove()
			delete(w.watchers, path)
		}
	}
	if w.native == nil {
		native, err := newNativeWatcher()
		if err != nil {
			w.switchToPolling()
			return false
		}
		w.native = native
	}
	added := false
	paths := make([]string, 0, len(wanted))
	for path := range wanted {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	for _, path := range paths {
		entry, ok := snapshot[path]
		if _, installed := w.watchers[path]; installed || !ok {
			continue
		}
		isLinked := linked[path]
		watched := path
		remove, err := w.native.add(path, func(name string) {
			if isLinked {
				w.onLinkedFileEvent(watched)
			} else {
				w.onEvent(watched, name)
			}
		})
		if err != nil {
			// Out of watches or unsupported: compare snapshots from now on,
			// and say coverage was uncertain.
			if !errors.Is(err, fs.ErrNotExist) && !isDenied(err) {
				w.switchToPolling()
				return false
			}
			continue
		}
		w.watchers[path] = installedWatch{remove: remove, dev: entry.dev, ino: entry.ino}
		added = true
	}
	return added
}

// switchToPolling compares snapshots from now on and reports an overflow;
// mu is held.
func (w *FileWatcher) switchToPolling() {
	if w.mode == "polling" {
		return
	}
	w.mode = "polling"
	for _, installed := range w.watchers {
		installed.remove()
	}
	w.watchers = map[string]installedWatch{}
	if w.timer != nil {
		w.timer.Stop()
		w.timer = nil
	}
	go func() {
		w.send(FileChange{Kind: "overflow"})
		w.schedulePoll()
	}()
}

// onEvent is a native event in directory about name ("" for the directory
// itself, as libuv reports its own name for it).
func (w *FileWatcher) onEvent(directory, name string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		return
	}
	path := directory
	if name != "" {
		if !strings.HasSuffix(path, string(filepath.Separator)) {
			path += string(filepath.Separator)
		}
		path += name
	}
	// Events about unrelated siblings of an ancestor, or about excluded
	// entries, are ignored.
	relevant := w.inScope(path)
	if relevant {
		w.events[w.reported(path)] = true
	}
	if relevant || name == "" {
		w.scheduleFlush()
	}
}

// onLinkedFileEvent: the file a target links to changed; report the target.
func (w *FileWatcher) onLinkedFileEvent(target string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		return
	}
	w.events[target] = true
	w.scheduleFlush()
}

func (w *FileWatcher) inScope(path string) bool {
	for _, target := range w.targets {
		if isWithin(target.path, path) {
			return true
		}
		if !isWithin(path, target.path) || path == target.path {
			continue
		}
		relative, err := filepath.Rel(target.path, path)
		if err != nil {
			continue
		}
		components := strings.Split(relative, string(filepath.Separator))
		if !target.recursive && len(components) > 1 {
			continue
		}
		excluded := false
		for _, name := range components {
			if target.excluded(name) {
				excluded = true
				break
			}
		}
		if !excluded {
			return true
		}
	}
	return false
}
