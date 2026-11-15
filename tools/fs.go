package tools

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/leocavalcante/pi-durable-go/internal/js"
)

// The file system of the local environment, ported from upstream's
// NodeExecutionEnv (@earendil-works/pi-durable/env/node): the same results
// and the same errors, Node's messages included (FileError).

// FileInfo is a file's metadata (upstream FileInfo): its name, path, kind
// (file, directory, or symlink), size, and modification time.
type FileInfo struct {
	Name    string  `json:"name"`
	Path    string  `json:"path"`
	Kind    string  `json:"kind"`
	Size    int64   `json:"size"`
	MtimeMs float64 `json:"mtimeMs"`
}

// fileInfoOf is the metadata of path, from its lstat; kinds other than
// files, directories, and symbolic links are unsupported.
func fileInfoOf(name string, info fs.FileInfo) (FileInfo, error) {
	var kind string
	switch mode := info.Mode(); {
	case mode.IsRegular():
		kind = "file"
	case mode.IsDir():
		kind = "directory"
	case mode&fs.ModeSymlink != 0:
		kind = "symlink"
	default:
		return FileInfo{}, &FileError{Code: "invalid", Message: "Unsupported file type", Path: name}
	}
	modified := info.ModTime()
	return FileInfo{Name: nodeBasename(name), Path: name, Kind: kind, Size: info.Size(),
		MtimeMs: float64(modified.Unix())*1000 + float64(modified.Nanosecond())/1e6}, nil
}

// nodeBasename is Node's path.basename: the last component, without
// trailing separators; empty for the root.
func nodeBasename(name string) string {
	trimmed := strings.TrimRight(name, "/")
	if trimmed == "" {
		return ""
	}
	return path.Base(trimmed)
}

// ID names the environment's file namespace (upstream FileSystem.id): every
// local environment sees the same files.
func (e *LocalEnv) ID() string { return "local" }

// JoinPath joins path parts as Node's path.join does: normalized, a
// trailing separator kept, "." for nothing.
func (e *LocalEnv) JoinPath(parts ...string) string {
	var kept []string
	for _, part := range parts {
		if part != "" {
			kept = append(kept, part)
		}
	}
	if len(kept) == 0 {
		return "."
	}
	joined := strings.Join(kept, "/")
	cleaned := path.Clean(joined)
	if strings.HasSuffix(joined, "/") && cleaned != "/" {
		if cleaned == "." {
			return "./"
		}
		cleaned += "/"
	}
	return cleaned
}

// ReadTextFile reads a file as UTF-8 text, as Node's readFile does: invalid
// bytes become U+FFFD and a byte-order mark stays.
func (e *LocalEnv) ReadTextFile(ctx context.Context, name string) (string, error) {
	data, err := e.ReadBinaryFile(ctx, name)
	if err != nil {
		return "", err
	}
	var decoder js.UTF8Decoder
	return decoder.Decode(data, false), nil
}

// ReadBinaryFile reads a file's bytes.
func (e *LocalEnv) ReadBinaryFile(ctx context.Context, name string) ([]byte, error) {
	resolved := e.resolve(name)
	if err := aborted(ctx, resolved); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(resolved)
	if err != nil {
		return nil, nodeError(err)
	}
	return data, nil
}

// AppendFile appends data to a file, creating it and its parent
// directories as needed.
func (e *LocalEnv) AppendFile(ctx context.Context, name string, data []byte) error {
	resolved := e.resolve(name)
	if err := aborted(ctx, resolved); err != nil {
		return err
	}
	if err := mkdirAll(filepath.Dir(resolved)); err != nil {
		return err
	}
	if err := aborted(ctx, resolved); err != nil {
		return err
	}
	f, err := os.OpenFile(resolved, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o666)
	if err != nil {
		return nodeError(err)
	}
	_, err = f.Write(data)
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nodeError(err)
	}
	return aborted(ctx, resolved)
}

// TruncateFile truncates or extends a file to exactly size bytes.
func (e *LocalEnv) TruncateFile(ctx context.Context, name string, size int64) error {
	resolved := e.resolve(name)
	if err := aborted(ctx, resolved); err != nil {
		return err
	}
	if size < 0 || size > maxSafeInteger {
		return &FileError{Code: "invalid", Message: "File size must be a non-negative safe integer", Path: resolved}
	}
	f, err := os.OpenFile(resolved, os.O_RDWR, 0)
	if err != nil {
		return nodeError(err)
	}
	defer f.Close()
	if err := f.Truncate(size); err != nil {
		return nodeError(err)
	}
	return aborted(ctx, resolved)
}

// FlushFile flushes a file's contents and the metadata needed to read them
// to storage.
func (e *LocalEnv) FlushFile(ctx context.Context, name string) error {
	resolved := e.resolve(name)
	if err := aborted(ctx, resolved); err != nil {
		return err
	}
	f, err := os.OpenFile(resolved, os.O_RDWR, 0)
	if err != nil {
		return nodeError(err)
	}
	defer f.Close()
	// POSIX refuses to open a directory for writing; Windows opens it.
	if info, err := f.Stat(); err == nil && info.IsDir() {
		return &FileError{Code: "is_directory", Message: "Is a directory", Path: resolved}
	}
	if err := f.Sync(); err != nil {
		return nodeError(err)
	}
	return aborted(ctx, resolved)
}

// RenameFile renames a file or directory; a failure names the source.
func (e *LocalEnv) RenameFile(ctx context.Context, source, destination string) error {
	from, to := e.resolve(source), e.resolve(destination)
	if err := aborted(ctx, to); err != nil {
		return err
	}
	return nodeError(rename(from, to))
}

// FileInfo is the metadata of a path itself, a symbolic link not followed.
func (e *LocalEnv) FileInfo(ctx context.Context, name string) (FileInfo, error) {
	resolved := e.resolve(name)
	if err := aborted(ctx, resolved); err != nil {
		return FileInfo{}, err
	}
	info, err := os.Lstat(resolved)
	if err != nil {
		return FileInfo{}, nodeError(err)
	}
	return fileInfoOf(resolved, info)
}

// ListDir lists a directory's entries in byte order of their names, as
// Node's readdir does; entries of other kinds are left out.
func (e *LocalEnv) ListDir(ctx context.Context, name string) ([]FileInfo, error) {
	resolved := e.resolve(name)
	if err := aborted(ctx, resolved); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(resolved)
	if err != nil {
		return nil, syscallError("scandir", resolved, err)
	}
	infos := []FileInfo{}
	for _, entry := range entries {
		if err := aborted(ctx, resolved); err != nil {
			return nil, err
		}
		entryPath := filepath.Join(resolved, entry.Name())
		info, err := os.Lstat(entryPath)
		if err != nil {
			return nil, nodeError(err)
		}
		if fileInfo, err := fileInfoOf(entryPath, info); err == nil {
			infos = append(infos, fileInfo)
		}
	}
	return infos, nil
}

// DirReader pages one directory's entries (upstream DirReader).
type DirReader struct {
	dir    *os.File
	path   string
	done   bool
	closed bool
}

// OpenDirReader opens a directory for reading its entries in pages.
func (e *LocalEnv) OpenDirReader(ctx context.Context, name string) (*DirReader, error) {
	resolved := e.resolve(name)
	if err := aborted(ctx, resolved); err != nil {
		return nil, err
	}
	dir, err := os.Open(resolved)
	if err != nil {
		return nil, syscallError("opendir", resolved, err)
	}
	if info, err := dir.Stat(); err != nil || !info.IsDir() {
		dir.Close()
		if err == nil {
			err = syscall.ENOTDIR
		}
		return nil, syscallError("opendir", resolved, err)
	}
	if err := aborted(ctx, resolved); err != nil {
		dir.Close()
		return nil, err
	}
	return &DirReader{dir: dir, path: resolved}, nil
}

// Next returns up to maxEntries entries in the order the file system
// returns them, continuing where the last call stopped, and whether the
// listing is done. Entries removed before their metadata is read are
// skipped, as are entries of other kinds.
func (r *DirReader) Next(ctx context.Context, maxEntries int) ([]FileInfo, bool, error) {
	if err := aborted(ctx, r.path); err != nil {
		return nil, false, err
	}
	if r.closed {
		return nil, false, &FileError{Code: "invalid", Message: "Directory reader is closed", Path: r.path}
	}
	if maxEntries <= 0 {
		return nil, false, &FileError{Code: "invalid", Message: "maxEntries must be a positive safe integer", Path: r.path}
	}
	entries := []FileInfo{}
	for !r.done && len(entries) < maxEntries {
		page, err := r.dir.ReadDir(1)
		if errors.Is(err, io.EOF) || err == nil && len(page) == 0 {
			r.done = true
			break
		}
		if err != nil {
			return nil, false, syscallError("readdir", r.path, err)
		}
		if err := aborted(ctx, r.path); err != nil {
			return nil, false, err
		}
		entryPath := filepath.Join(r.path, page[0].Name())
		info, err := os.Lstat(entryPath)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				continue
			}
			return nil, false, nodeError(err)
		}
		if fileInfo, err := fileInfoOf(entryPath, info); err == nil {
			entries = append(entries, fileInfo)
		}
	}
	return entries, r.done, nil
}

// Close closes the reader.
func (r *DirReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	_ = r.dir.Close()
	return nil
}

// CanonicalPath is a path with its symbolic links resolved (realpath).
func (e *LocalEnv) CanonicalPath(ctx context.Context, name string) (string, error) {
	resolved := e.resolve(name)
	if err := aborted(ctx, resolved); err != nil {
		return "", err
	}
	canonical, err := filepath.EvalSymlinks(resolved)
	if err != nil {
		return "", realpathError(resolved, err)
	}
	return canonical, nil
}

// CreateDir creates a directory, and its parents when recursive.
func (e *LocalEnv) CreateDir(ctx context.Context, name string, recursive bool) error {
	resolved := e.resolve(name)
	if err := aborted(ctx, resolved); err != nil {
		return err
	}
	if !recursive {
		return nodeError(os.Mkdir(resolved, 0o777))
	}
	return mkdirAll(resolved)
}

// Remove removes a file, or a directory with everything below it when
// recursive, as Node's rm does: a directory is refused without recursive,
// and force ignores a missing path.
func (e *LocalEnv) Remove(ctx context.Context, name string, recursive, force bool) error {
	resolved := e.resolve(name)
	if err := aborted(ctx, resolved); err != nil {
		return err
	}
	info, err := os.Lstat(resolved)
	if err != nil {
		if force && errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return nodeError(err)
	}
	if info.IsDir() {
		if !recursive {
			return &FileError{Code: "unknown", Message: "Path is a directory: rm returned EISDIR (is a directory) " + resolved, Path: resolved, Err: syscall.EISDIR}
		}
		return nodeError(os.RemoveAll(resolved))
	}
	return nodeError(os.Remove(resolved))
}

// Cleanup ends the processes the environment started that still run, with
// their process groups.
func (e *LocalEnv) Cleanup(ctx context.Context) {
	e.processes.Lock()
	running := make([]func() error, 0, len(e.processes.running))
	for _, cancel := range e.processes.running {
		running = append(running, cancel)
	}
	e.processes.running = nil
	e.processes.Unlock()
	for _, cancel := range running {
		_ = cancel()
	}
}

// maxSafeInteger is JavaScript's Number.MAX_SAFE_INTEGER.
const maxSafeInteger = 1<<53 - 1
