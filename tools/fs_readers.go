package tools

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"syscall"
)

// TextLine is one line of a text file (upstream TextLine): its text without
// the newline, and whether a newline ended it.
type TextLine struct {
	Text       string `json:"text"`
	Terminated bool   `json:"terminated"`
}

// TextLineReader reads a text file line by line, split at "\n" only
// (upstream NodeTextLineReader): decoded as UTF-8, a leading byte-order
// mark dropped.
type TextLineReader struct {
	file     *os.File
	path     string
	decoder  streamDecoder
	chunk    []byte
	offset   int64
	buffered string
	ended    bool
	closed   bool
}

// OpenTextLineReader opens a text file for reading line by line.
func (e *LocalEnv) OpenTextLineReader(ctx context.Context, name string) (*TextLineReader, error) {
	resolved := e.resolve(name)
	if err := aborted(ctx, resolved); err != nil {
		return nil, err
	}
	f, err := os.Open(resolved)
	if err != nil {
		return nil, nodeError(err)
	}
	if err := aborted(ctx, resolved); err != nil {
		f.Close()
		return nil, err
	}
	return &TextLineReader{file: f, path: resolved, chunk: make([]byte, 64*1024)}, nil
}

// ReadLine returns the next line; ok is false at the end of the file.
func (r *TextLineReader) ReadLine(ctx context.Context) (line TextLine, ok bool, err error) {
	if err := aborted(ctx, r.path); err != nil {
		return TextLine{}, false, err
	}
	if r.closed {
		return TextLine{}, false, &FileError{Code: "invalid", Message: "Text line reader is closed", Path: r.path}
	}
	for {
		if newline := strings.IndexByte(r.buffered, '\n'); newline != -1 {
			text := r.buffered[:newline]
			r.buffered = r.buffered[newline+1:]
			return TextLine{Text: text, Terminated: true}, true, nil
		}
		if r.ended {
			if r.buffered == "" {
				return TextLine{}, false, nil
			}
			text := r.buffered
			r.buffered = ""
			return TextLine{Text: text}, true, nil
		}
		// Explicit positions let an aborted read be retried without
		// skipping bytes.
		n, err := r.file.ReadAt(r.chunk, r.offset)
		if err != nil && !errors.Is(err, io.EOF) {
			return TextLine{}, false, nodeError(err)
		}
		if err := aborted(ctx, r.path); err != nil {
			return TextLine{}, false, err
		}
		r.offset += int64(n)
		if n == 0 {
			r.buffered += r.decoder.decode(nil, true)
			r.ended = true
		} else {
			r.buffered += r.decoder.decode(r.chunk[:n], false)
		}
	}
}

// Close closes the reader.
func (r *TextLineReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.buffered = ""
	_ = r.file.Close()
	return nil
}

// ReadTextLines reads a text file's lines, at most maxLines of them;
// maxLines < 0 reads all, and 0 none.
func (e *LocalEnv) ReadTextLines(ctx context.Context, name string, maxLines int) ([]string, error) {
	if maxLines == 0 {
		return []string{}, nil
	}
	reader, err := e.OpenTextLineReader(ctx, name)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	lines := []string{}
	for maxLines < 0 || len(lines) < maxLines {
		line, ok, err := reader.ReadLine(ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		lines = append(lines, line.Text)
	}
	return lines, nil
}

// binaryReadChunk is the most a binary read asks of the file at once.
const binaryReadChunk = 1024 * 1024

// BinaryReader reads positionally from one opened regular file (upstream
// BinaryReader): all calls see the same file even if its path is renamed.
type BinaryReader struct {
	file   *os.File
	path   string
	closed bool
}

// OpenBinaryReader opens a regular file for bounded positional reads. A
// directory fails with is_directory, other files that are not regular with
// invalid; with noFollow, a symbolic link as the last path component is
// refused.
func (e *LocalEnv) OpenBinaryReader(ctx context.Context, name string, noFollow bool) (*BinaryReader, error) {
	resolved := e.resolve(name)
	if err := aborted(ctx, resolved); err != nil {
		return nil, err
	}
	f, err := openForReading(resolved, noFollow)
	if err != nil {
		// O_NOFOLLOW reports a symbolic link as ELOOP (EMLINK on some BSDs).
		if noFollow && (errors.Is(err, syscall.ELOOP) || errors.Is(err, syscall.EMLINK)) {
			return nil, &FileError{Code: "invalid", Message: "Refusing to follow a symbolic link", Path: resolved, Err: err}
		}
		if refused := (*symlinkRefusal)(nil); errors.As(err, &refused) {
			return nil, &FileError{Code: "invalid", Message: "Refusing to follow a symbolic link", Path: resolved}
		}
		return nil, nodeError(err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nodeError(err)
	}
	if !info.Mode().IsRegular() {
		f.Close()
		if info.IsDir() {
			return nil, &FileError{Code: "is_directory", Message: "EISDIR: illegal operation on a directory, read", Path: resolved}
		}
		return nil, &FileError{Code: "invalid", Message: "Not a regular file", Path: resolved}
	}
	if err := aborted(ctx, resolved); err != nil {
		f.Close()
		return nil, err
	}
	return &BinaryReader{file: f, path: resolved}, nil
}

// symlinkRefusal is a symbolic link refused where O_NOFOLLOW is missing.
type symlinkRefusal struct{}

func (*symlinkRefusal) Error() string { return "Refusing to follow a symbolic link" }

func (r *BinaryReader) check(ctx context.Context) error {
	if err := aborted(ctx, r.path); err != nil {
		return err
	}
	if r.closed {
		return &FileError{Code: "invalid", Message: "Binary reader is closed", Path: r.path}
	}
	return nil
}

// Info is the metadata of the opened file, not of whatever its path names
// now.
func (r *BinaryReader) Info(ctx context.Context) (FileInfo, error) {
	if err := r.check(ctx); err != nil {
		return FileInfo{}, err
	}
	info, err := r.file.Stat()
	if err != nil {
		return FileInfo{}, nodeError(err)
	}
	return fileInfoOf(r.path, info)
}

// Read returns up to length bytes at offset; fewer only at the end of the
// file.
func (r *BinaryReader) Read(ctx context.Context, offset, length int64) ([]byte, error) {
	if err := r.check(ctx); err != nil {
		return nil, err
	}
	if offset < 0 || offset > maxSafeInteger || length < 0 || length > maxSafeInteger {
		return nil, &FileError{Code: "invalid", Message: "Offset and length must be non-negative safe integers", Path: r.path}
	}
	var out bytes.Buffer
	for int64(out.Len()) < length {
		chunk := make([]byte, min(length-int64(out.Len()), binaryReadChunk))
		n, err := r.file.ReadAt(chunk, offset+int64(out.Len()))
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, nodeError(err)
		}
		if err := aborted(ctx, r.path); err != nil {
			return nil, err
		}
		if n == 0 {
			break
		}
		out.Write(chunk[:n])
	}
	return out.Bytes(), nil
}

// ScanLines locates lines [startLine, endLine) in one pass over the file
// (upstream BinaryReader.scanLines); endLine 0 scans to the end.
func (r *BinaryReader) ScanLines(ctx context.Context, startLine, endLine int64) (LineScan, error) {
	if err := r.check(ctx); err != nil {
		return LineScan{}, err
	}
	scanner, err := newLineScanner(startLine, endLine)
	if err != nil {
		return LineScan{}, &FileError{Code: "invalid", Message: "Invalid line range", Path: r.path}
	}
	chunk := make([]byte, 64*1024)
	for position := int64(0); ; {
		n, err := r.file.ReadAt(chunk, position)
		if err != nil && !errors.Is(err, io.EOF) {
			return LineScan{}, nodeError(err)
		}
		if err := aborted(ctx, r.path); err != nil {
			return LineScan{}, err
		}
		if n == 0 {
			return scanner.finish(), nil
		}
		scanner.push(chunk[:n])
		position += int64(n)
	}
}

// Close closes the reader.
func (r *BinaryReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	_ = r.file.Close()
	return nil
}
