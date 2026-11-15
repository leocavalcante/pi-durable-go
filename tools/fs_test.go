package tools

import (
	"context"
	"errors"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/leocavalcante/pi-durable-go/internal/js"
)

// Upstream's reader conformance cases (testing/env-conformance.ts) on a
// local environment.

func fileCode(err error) string {
	var fileError *FileError
	if errors.As(err, &fileError) {
		return fileError.Code
	}
	if err == nil {
		return ""
	}
	return "other: " + err.Error()
}

func canceled() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func testLocalEnv(t *testing.T) *LocalEnv {
	t.Helper()
	env, err := NewLocalEnv(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func TestBinaryReaderReadsByteRangesOfTheOpenedFile(t *testing.T) {
	env, ctx := testLocalEnv(t), context.Background()
	writeText(t, env, "data.txt", "hello world")
	reader, err := env.OpenBinaryReader(ctx, "data.txt", false)
	must(t, err)
	info, err := reader.Info(ctx)
	if err != nil || info.Name != "data.txt" || info.Kind != "file" || info.Size != 11 {
		t.Fatalf("info %+v %v", info, err)
	}
	for _, c := range []struct {
		offset, length int64
		want           string
	}{{0, 5, "hello"}, {6, 100, "world"}, {11, 4, ""}, {50, 1, ""}, {3, 0, ""}} {
		if got, err := reader.Read(ctx, c.offset, c.length); err != nil || string(got) != c.want {
			t.Fatalf("read(%d, %d) = %q %v", c.offset, c.length, got, err)
		}
	}
	if _, err := reader.Read(ctx, -1, 1); fileCode(err) != "invalid" {
		t.Fatalf("negative offset: %v", err)
	}
	if _, err := reader.Read(canceled(), 0, 1); fileCode(err) != "aborted" {
		t.Fatalf("aborted read: %v", err)
	}
	must(t, reader.Close())
	must(t, reader.Close())
	if _, err := reader.Read(ctx, 0, 1); fileCode(err) != "invalid" {
		t.Fatalf("read after close: %v", err)
	}
	if _, err := reader.Info(ctx); fileCode(err) != "invalid" {
		t.Fatalf("info after close: %v", err)
	}
}

func TestBinaryReaderScansLinesLikeDecodingTheWholeFile(t *testing.T) {
	env, ctx := testLocalEnv(t), context.Background()
	// A byte-order mark, an invalid sequence before a newline, an empty
	// line, a later U+FEFF, and no final newline.
	data := []byte{0xef, 0xbb, 0xbf, 0x61, 0x0a, 0xe2, 0x82, 0x0a, 0x0a, 0xef, 0xbb, 0xbf, 0x62, 0x0a, 0xc3, 0xa9}
	must(t, env.WriteFile(ctx, "lines.txt", data))
	decode := func(from, to int64) string {
		var decoder js.UTF8Decoder
		text := decoder.Decode(data[from:to], false)
		if from == 0 {
			text = strings.TrimPrefix(text, "\ufeff")
		}
		return text
	}
	lines := strings.Split(decode(0, int64(len(data))), "\n")
	reader, err := env.OpenBinaryReader(ctx, "lines.txt", false)
	must(t, err)
	defer reader.Close()
	for _, r := range [][2]int64{{0, 0}, {0, 1}, {1, 3}, {2, 3}, {3, 0}, {4, 9}} {
		scan, err := reader.ScanLines(ctx, r[0], r[1])
		must(t, err)
		end := int64(len(lines))
		if r[1] != 0 {
			end = min(r[1], end)
		}
		var selected []string
		if r[0] < int64(len(lines)) {
			selected = lines[r[0]:end]
		}
		joined := strings.Join(selected, "\n")
		first := ""
		if r[0] < int64(len(lines)) {
			first = lines[r[0]]
		}
		if scan.Newlines != int64(len(lines)-1) || decode(scan.Start, scan.End) != joined || scan.SelectedBytes != int64(len(joined)) ||
			decode(scan.Start, scan.FirstLineEnd) != first || scan.FirstLineBytes != int64(len(first)) {
			t.Fatalf("scan %v: %+v", r, scan)
		}
	}
	if scan, err := reader.ScanLines(ctx, 9, 0); err != nil || scan.Start != int64(len(data)) || scan.End != int64(len(data)) || scan.SelectedBytes != 0 {
		t.Fatalf("past the end: %+v %v", scan, err)
	}
	if _, err := reader.ScanLines(ctx, 2, 2); fileCode(err) != "invalid" {
		t.Fatalf("empty range: %v", err)
	}
}

func TestBinaryReaderKeepsReadingTheFileItOpenedAfterARename(t *testing.T) {
	env, ctx := testLocalEnv(t), context.Background()
	writeText(t, env, "a.txt", "one")
	reader, err := env.OpenBinaryReader(ctx, "a.txt", false)
	must(t, err)
	defer reader.Close()
	must(t, env.RenameFile(ctx, "a.txt", "b.txt"))
	writeText(t, env, "a.txt", "two")
	if got, err := reader.Read(ctx, 0, 10); err != nil || string(got) != "one" {
		t.Fatalf("read %q %v", got, err)
	}
}

func TestBinaryReaderRefusesDirectoriesMissingFilesAndAbortedOpens(t *testing.T) {
	env, ctx := testLocalEnv(t), context.Background()
	must(t, env.CreateDir(ctx, "dir", true))
	writeText(t, env, "file.txt", "x")
	if _, err := env.OpenBinaryReader(ctx, "dir", false); fileCode(err) != "is_directory" {
		t.Fatalf("directory: %v", err)
	}
	if _, err := env.OpenBinaryReader(ctx, "missing.txt", false); fileCode(err) != "not_found" {
		t.Fatalf("missing: %v", err)
	}
	if _, err := env.OpenBinaryReader(canceled(), "file.txt", false); fileCode(err) != "aborted" {
		t.Fatalf("aborted: %v", err)
	}
}

func TestDirectoryReaderPagesEveryEntryExactlyOnce(t *testing.T) {
	env, ctx := testLocalEnv(t), context.Background()
	names := []string{"a.txt", "b.txt", "c.txt", "d.txt", "e.txt"}
	for _, name := range names {
		writeText(t, env, name, name)
	}
	must(t, env.CreateDir(ctx, "sub", true))
	reader, err := env.OpenDirReader(ctx, ".")
	must(t, err)
	defer reader.Close()
	var entries []FileInfo
	for done := false; !done; {
		var page []FileInfo
		page, done, err = reader.Next(ctx, 2)
		must(t, err)
		if len(page) > 2 {
			t.Fatalf("page of %d", len(page))
		}
		entries = append(entries, page...)
	}
	var got []string
	for _, entry := range entries {
		got = append(got, entry.Name)
		if entry.Name == "sub" && entry.Kind != "directory" || entry.Name == "a.txt" && (entry.Kind != "file" || entry.Size != 5) {
			t.Fatalf("entry %+v", entry)
		}
	}
	sort.Strings(got)
	if want := append(names, "sub"); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("entries %v", got)
	}
}

func TestDirectoryReaderReportsTheEndAndRefusesUseAfterClose(t *testing.T) {
	env, ctx := testLocalEnv(t), context.Background()
	must(t, env.CreateDir(ctx, "empty", true))
	reader, err := env.OpenDirReader(ctx, "empty")
	must(t, err)
	for range 2 {
		if entries, done, err := reader.Next(ctx, 10); err != nil || len(entries) != 0 || !done {
			t.Fatalf("end: %v %v %v", entries, done, err)
		}
	}
	if _, _, err := reader.Next(ctx, 0); fileCode(err) != "invalid" {
		t.Fatalf("zero: %v", err)
	}
	if _, _, err := reader.Next(canceled(), 1); fileCode(err) != "aborted" {
		t.Fatalf("aborted: %v", err)
	}
	must(t, reader.Close())
	must(t, reader.Close())
	if _, _, err := reader.Next(ctx, 1); fileCode(err) != "invalid" {
		t.Fatalf("closed: %v", err)
	}
}

func TestDirectoryReaderRefusesMissingPathsAndFiles(t *testing.T) {
	env, ctx := testLocalEnv(t), context.Background()
	writeText(t, env, "file.txt", "x")
	if _, err := env.OpenDirReader(ctx, "missing"); fileCode(err) != "not_found" {
		t.Fatalf("missing: %v", err)
	}
	if _, err := env.OpenDirReader(ctx, "file.txt"); fileCode(err) != "not_directory" {
		t.Fatalf("file: %v", err)
	}
	if _, err := env.OpenDirReader(canceled(), "."); fileCode(err) != "aborted" {
		t.Fatalf("aborted: %v", err)
	}
}

func TestDirectoryReaderSkipsEntriesRemovedDuringEnumeration(t *testing.T) {
	env, ctx := testLocalEnv(t), context.Background()
	must(t, env.CreateDir(ctx, "dir", true))
	for _, name := range []string{"x", "y", "z"} {
		writeText(t, env, "dir/"+name, name)
	}
	reader, err := env.OpenDirReader(ctx, "dir")
	must(t, err)
	defer reader.Close()
	for _, name := range []string{"x", "y", "z"} {
		must(t, env.Remove(ctx, "dir/"+name, false, false))
	}
	var entries []FileInfo
	for page := 0; page < 10; page++ {
		next, done, err := reader.Next(ctx, 10)
		must(t, err)
		entries = append(entries, next...)
		if done {
			break
		}
	}
	if len(entries) != 0 {
		t.Fatalf("entries %+v", entries)
	}
}

func TestBinaryReaderFollowsSymlinksUnlessNoFollowRefusesTheFinalOne(t *testing.T) {
	env, ctx := testLocalEnv(t), context.Background()
	writeText(t, env, "target.txt", "target")
	writeText(t, env, "sub/inner.txt", "inner")
	must(t, os.Symlink("target.txt", env.resolve("link.txt")))
	must(t, os.Symlink("sub", env.resolve("dirlink")))
	followed, err := env.OpenBinaryReader(ctx, "link.txt", false)
	must(t, err)
	if got, err := followed.Read(ctx, 0, 10); err != nil || string(got) != "target" {
		t.Fatalf("followed %q %v", got, err)
	}
	followed.Close()
	if _, err := env.OpenBinaryReader(ctx, "link.txt", true); fileCode(err) != "invalid" {
		t.Fatalf("noFollow: %v", err)
	}
	// Only the final component is refused; earlier symlinked directories
	// still resolve.
	inner, err := env.OpenBinaryReader(ctx, "dirlink/inner.txt", true)
	must(t, err)
	defer inner.Close()
	if got, err := inner.Read(ctx, 0, 10); err != nil || string(got) != "inner" {
		t.Fatalf("inner %q %v", got, err)
	}
}
