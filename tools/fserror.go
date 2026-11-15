package tools

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"syscall"
)

// Node's file errors. Upstream's tools run over a Node environment, whose
// file errors carry Node's messages, and pass them to the model: "ENOENT: no
// such file or directory, open '/path'". The local environment reports its
// errors the same way; the original error stays wrapped for errors.Is.

// FileError is a failed file operation (upstream FileError): Code is one of
// aborted, not_found, permission_denied, not_directory, is_directory,
// invalid, not_supported, or unknown; the message is Node's; Path is the
// path involved, when there is one.
type FileError struct {
	Code    string
	Message string
	Path    string
	Err     error
}

func (e *FileError) Error() string { return e.Message }
func (e *FileError) Unwrap() error { return e.Err }

// fileErrorCodes are upstream toFileError's codes by Node's error code.
var fileErrorCodes = map[string]string{
	"ENOENT": "not_found", "EACCES": "permission_denied", "EPERM": "permission_denied",
	"ENOTDIR": "not_directory", "EISDIR": "is_directory", "EINVAL": "invalid",
}

// newFileError is a FileError with Node's message for code.
func newFileError(code, message, path string, err error) *FileError {
	fileCode, ok := fileErrorCodes[code]
	if !ok {
		fileCode = "unknown"
	}
	return &FileError{Code: fileCode, Message: code + ": " + message, Path: path, Err: err}
}

// aborted is the error of an operation whose context ended (upstream
// abortResult).
func aborted(ctx context.Context, path string) error {
	if ctx.Err() == nil {
		return nil
	}
	return &FileError{Code: "aborted", Message: "aborted", Path: path, Err: ctx.Err()}
}

// errnoMessages are libuv's messages (uv_strerror), which Node's fs errors
// use, by code.
var errnoMessages = []struct {
	errno   syscall.Errno
	code    string
	message string
}{
	{syscall.ENOENT, "ENOENT", "no such file or directory"},
	{syscall.EACCES, "EACCES", "permission denied"},
	{syscall.EPERM, "EPERM", "operation not permitted"},
	{syscall.EISDIR, "EISDIR", "illegal operation on a directory"},
	{syscall.ENOTDIR, "ENOTDIR", "not a directory"},
	{syscall.EEXIST, "EEXIST", "file already exists"},
	{syscall.ELOOP, "ELOOP", "too many symbolic links encountered"},
	{syscall.ENAMETOOLONG, "ENAMETOOLONG", "name too long"},
	{syscall.EROFS, "EROFS", "read-only file system"},
	{syscall.ENOSPC, "ENOSPC", "no space left on device"},
	{syscall.EBUSY, "EBUSY", "resource busy or locked"},
	{syscall.EMFILE, "EMFILE", "too many open files"},
	{syscall.ENFILE, "ENFILE", "file table overflow"},
	{syscall.EINVAL, "EINVAL", "invalid argument"},
	{syscall.EIO, "EIO", "i/o error"},
	{syscall.EXDEV, "EXDEV", "cross-device link not permitted"},
	{syscall.ENOTEMPTY, "ENOTEMPTY", "directory not empty"},
	{syscall.ETXTBSY, "ETXTBSY", "text file is busy"},
	{syscall.EFBIG, "EFBIG", "file too large"},
	{syscall.EAGAIN, "EAGAIN", "resource temporarily unavailable"},
	{syscall.EBADF, "EBADF", "bad file descriptor"},
	{syscall.EINTR, "EINTR", "interrupted system call"},
	{syscall.ENOMEM, "ENOMEM", "not enough memory"},
	{syscall.ENODEV, "ENODEV", "no such device"},
	{syscall.ENXIO, "ENXIO", "no such device or address"},
	{syscall.ESPIPE, "ESPIPE", "invalid seek"},
	{syscall.EMLINK, "EMLINK", "too many links"},
	{syscall.ENOSYS, "ENOSYS", "function not implemented"},
	{syscall.EPIPE, "EPIPE", "broken pipe"},
}

// errnoOf is an error's code and libuv message; ok is false for an error
// that is no system error.
func errnoOf(err error) (code, message string, ok bool) {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		for _, known := range errnoMessages {
			if known.errno == errno {
				return known.code, known.message, true
			}
		}
	}
	// Other platforms' codes, by class.
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return "ENOENT", "no such file or directory", true
	case errors.Is(err, fs.ErrPermission):
		return "EACCES", "permission denied", true
	case errors.Is(err, fs.ErrExist):
		return "EEXIST", "file already exists", true
	}
	return "", "", false
}

// nodeSyscalls name Go's operations as Node names its calls; the ones on a
// file descriptor carry no path.
var nodeSyscalls = map[string]struct {
	name     string
	withPath bool
}{
	"open": {"open", true}, "stat": {"stat", true}, "lstat": {"lstat", true}, "mkdir": {"mkdir", true},
	"readlink": {"readlink", true}, "chmod": {"chmod", true}, "mkdirtemp": {"mkdtemp", true}, "realpath": {"realpath", true},
	"read": {"read", false}, "write": {"write", false}, "close": {"close", false}, "seek": {"read", false},
	"sync": {"fsync", false}, "fstat": {"fstat", false}, "truncate": {"ftruncate", false},
}

// nodeError is err with Node's message when it is a file error Node would
// report with one.
func nodeError(err error) error {
	if err == nil {
		return nil
	}
	var already *FileError
	if errors.As(err, &already) {
		return err
	}
	var linkError *os.LinkError
	if errors.As(err, &linkError) {
		code, message, ok := errnoOf(linkError.Err)
		if !ok {
			return err
		}
		// Node names the source as the error's path.
		return newFileError(code, message+", "+linkError.Op+" '"+linkError.Old+"' -> '"+linkError.New+"'", linkError.Old, err)
	}
	var pathError *fs.PathError
	if !errors.As(err, &pathError) {
		return err
	}
	code, message, ok := errnoOf(pathError.Err)
	if !ok {
		return err
	}
	call, known := nodeSyscalls[pathError.Op]
	if !known {
		call.name, call.withPath = pathError.Op, true
	}
	text := message + ", " + call.name
	if call.withPath {
		text += " '" + pathError.Path + "'"
	}
	return newFileError(code, text, pathError.Path, err)
}

// syscallError is err as Node reports a failed call on path.
func syscallError(call, path string, err error) error {
	code, message, ok := errnoOf(err)
	if !ok {
		return err
	}
	return newFileError(code, message+", "+call+" '"+path+"'", path, err)
}

// realpathError is the error Node's realpath reports for path when Go
// cannot resolve it: the call on the whole path.
func realpathError(path string, err error) error {
	return syscallError("realpath", path, err)
}

// nodeFile reports its reads' errors as Node does.
type nodeFile struct{ *os.File }

func (f nodeFile) Read(p []byte) (int, error) {
	n, err := f.File.Read(p)
	if err != nil && err != io.EOF {
		err = nodeError(err)
	}
	return n, err
}
