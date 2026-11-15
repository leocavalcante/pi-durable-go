package tools

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"testing"
)

// envSetup is the files an env ops case starts with: directories, files
// with base64 contents, and symbolic links.
type envSetup struct {
	Dirs  []string          `json:"dirs,omitempty"`
	Files map[string]string `json:"files,omitempty"`
	Links map[string]string `json:"links,omitempty"`
}

func (s envSetup) create(t *testing.T, dir string) {
	t.Helper()
	for _, d := range s.Dirs {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for name, content := range s.Files {
		data, err := base64.StdEncoding.DecodeString(content)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(filepath.Join(dir, name)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for name, target := range s.Links {
		if err := os.Symlink(target, filepath.Join(dir, name)); err != nil {
			t.Fatal(err)
		}
	}
}

// runEnvOps plays env ops on a local environment in dir and reports them
// as the envOps oracle op reports NodeExecutionEnv's.
func runEnvOps(t *testing.T, dir string, ops [][]any) json.RawMessage {
	t.Helper()
	env, err := NewLocalEnv(dir)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	failure := func(err error) map[string]any {
		var fileError *FileError
		if !errors.As(err, &fileError) {
			return map[string]any{"error": map[string]any{"code": "go", "message": err.Error(), "path": nil}}
		}
		var path any
		if fileError.Path != "" {
			path = fileError.Path
		}
		return map[string]any{"error": map[string]any{"code": fileError.Code, "message": fileError.Message, "path": path}}
	}
	result := func(value any, err error) map[string]any {
		if err != nil {
			return failure(err)
		}
		return map[string]any{"value": value}
	}
	info := func(f FileInfo) map[string]any {
		return map[string]any{"name": f.Name, "path": f.Path, "kind": f.Kind, "size": f.Size, "mtimeMs": f.MtimeMs != 0}
	}
	str := func(v any) string { s, _ := v.(string); return s }
	num := func(v any) int64 {
		switch n := v.(type) {
		case int:
			return int64(n)
		case float64:
			return int64(n)
		}
		return 0
	}
	out := []any{}
	for _, op := range ops {
		args := op[1:]
		switch op[0] {
		case "fileInfo":
			f, err := env.FileInfo(ctx, str(args[0]))
			if err != nil {
				out = append(out, failure(err))
			} else {
				out = append(out, result(info(f), nil))
			}
		case "listDir":
			list, err := env.ListDir(ctx, str(args[0]))
			if err != nil {
				out = append(out, failure(err))
				break
			}
			infos := []any{}
			for _, f := range list {
				infos = append(infos, info(f))
			}
			out = append(out, result(infos, nil))
		case "dirReader":
			reader, err := env.OpenDirReader(ctx, str(args[0]))
			if err != nil {
				out = append(out, failure(err))
				break
			}
			names := []string{}
			pages := 0
			var failed any
			for {
				entries, done, err := reader.Next(ctx, int(num(args[1])))
				if err != nil {
					failed = failure(err)
					break
				}
				pages++
				for _, entry := range entries {
					names = append(names, entry.Name+":"+entry.Kind)
				}
				if done {
					break
				}
			}
			_, _, invalidErr := reader.Next(ctx, 0)
			reader.Close()
			_, _, closedErr := reader.Next(ctx, 1)
			sort.Strings(names)
			out = append(out, map[string]any{"names": names, "pages": pages, "failure": failed, "invalid": failure(invalidErr), "closed": failure(closedErr)})
		case "canonicalPath":
			out = append(out, result(env.CanonicalPath(ctx, str(args[0]))))
		case "exists":
			out = append(out, result(env.Exists(ctx, str(args[0]))))
		case "createDir":
			out = append(out, result(nil, env.CreateDir(ctx, str(args[0]), args[1] == true)))
		case "remove":
			out = append(out, result(nil, env.Remove(ctx, str(args[0]), args[1] == true, args[2] == true)))
		case "appendFile":
			out = append(out, result(nil, env.AppendFile(ctx, str(args[0]), []byte(str(args[1])))))
		case "truncateFile":
			out = append(out, result(nil, env.TruncateFile(ctx, str(args[0]), num(args[1]))))
		case "flushFile":
			out = append(out, result(nil, env.FlushFile(ctx, str(args[0]))))
		case "renameFile":
			out = append(out, result(nil, env.RenameFile(ctx, str(args[0]), str(args[1]))))
		case "readTextFile":
			out = append(out, result(env.ReadTextFile(ctx, str(args[0]))))
		case "readBinaryFile":
			data, err := env.ReadBinaryFile(ctx, str(args[0]))
			out = append(out, result(base64.StdEncoding.EncodeToString(data), err))
		case "readTextLines":
			maxLines := -1
			if args[1] != nil {
				maxLines = int(num(args[1]))
			}
			out = append(out, result(env.ReadTextLines(ctx, str(args[0]), maxLines)))
		case "textLines":
			reader, err := env.OpenTextLineReader(ctx, str(args[0]))
			if err != nil {
				out = append(out, failure(err))
				break
			}
			lines := []any{}
			for {
				line, ok, err := reader.ReadLine(ctx)
				if err != nil {
					lines = append(lines, failure(err))
					break
				}
				if !ok {
					break
				}
				lines = append(lines, line)
			}
			reader.Close()
			_, _, closedErr := reader.ReadLine(ctx)
			out = append(out, append(lines, failure(closedErr)))
		case "binary":
			reader, err := env.OpenBinaryReader(ctx, str(args[0]), args[1] == true)
			if err != nil {
				out = append(out, failure(err))
				break
			}
			f, err := reader.Info(ctx)
			steps := []any{}
			if err != nil {
				steps = append(steps, failure(err))
			} else {
				steps = append(steps, result(info(f), nil))
			}
			reads, _ := args[2].([]any)
			for _, r := range reads {
				pair := r.([]any)
				data, err := reader.Read(ctx, num(pair[0]), num(pair[1]))
				steps = append(steps, result(base64.StdEncoding.EncodeToString(data), err))
			}
			scans, _ := args[3].([]any)
			for _, r := range scans {
				pair := r.([]any)
				end := int64(0)
				if pair[1] != nil {
					end = num(pair[1])
				}
				steps = append(steps, result(reader.ScanLines(ctx, num(pair[0]), end)))
			}
			reader.Close()
			_, err = reader.Read(ctx, 0, 1)
			out = append(out, append(steps, failure(err)))
		case "joinPath":
			var parts []string
			for _, p := range args[0].([]any) {
				parts = append(parts, p.(string))
			}
			out = append(out, result(env.JoinPath(parts...), nil))
		default:
			t.Fatalf("unknown env op %v", op[0])
		}
	}
	data, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	return json.RawMessage(strings.ReplaceAll(string(data), dir, "<dir>"))
}

// The local environment's file system answers as upstream's
// NodeExecutionEnv does, with Node's messages: its answers were recorded
// by TestCrosscheckEnvOps (PI_DURABLE_CROSSCHECK_RECORD=1).
func TestEnvOpsAsNodes(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the recorded messages are Linux's")
	}
	if os.Geteuid() == 0 {
		t.Skip("permissions do not apply to root")
	}
	data, err := os.ReadFile(filepath.Join("testdata", "env-ops-upstream.json"))
	if err != nil {
		t.Fatal(err)
	}
	var recorded struct {
		Setup envSetup          `json:"setup"`
		Ops   [][]any           `json:"ops"`
		Want  []json.RawMessage `json:"want"`
	}
	if err := json.Unmarshal(data, &recorded); err != nil {
		t.Fatal(err)
	}
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	recorded.Setup.create(t, dir)
	var got []json.RawMessage
	if err := json.Unmarshal(runEnvOps(t, dir, recorded.Ops), &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(recorded.Want) || len(got) < 80 {
		t.Fatalf("%d results, %d recorded", len(got), len(recorded.Want))
	}
	for i := range got {
		var a, b any
		_ = json.Unmarshal(got[i], &a)
		_ = json.Unmarshal(recorded.Want[i], &b)
		if !reflect.DeepEqual(a, b) {
			t.Errorf("%v:\n got %s\nwant %s", recorded.Ops[i], got[i], recorded.Want[i])
		}
	}
}
