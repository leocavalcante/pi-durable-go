package tools

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"strings"
	"syscall"
	"time"

	durable "github.com/leocavalcante/pi-durable-go"
	"github.com/leocavalcante/pi-durable-go/internal/jsnum"
)

const maxReadBytes = 50 * 1024
const maxReadLines = 2000

// Coding returns read, write, edit, and bash tools. They are not replay-safe:
// interrupted operations may have had effects and are never silently retried.
// PowerShell returns the powershell tool: like bash, through PowerShell
// instead of the environment's shell, trying programs in order (default pwsh,
// then powershell); a program that cannot start is skipped. The environment
// must implement ArgvExecutor, as LocalEnv does.
func PowerShell(programs ...string) durable.Extension {
	return PowerShellWith(ShellToolOptions{Programs: programs})
}

// PowerShellWith returns the powershell tool with options (upstream
// createPowerShellTool).
func PowerShellWith(options ShellToolOptions) durable.Extension {
	return durable.Extension{Name: "powershell-tool", Tools: []durable.ToolDefinition{
		{ToolSpec: durable.ToolSpec{Name: "powershell", Description: "Execute a PowerShell command in the current working directory. Returns combined stdout and stderr. Output is truncated to last 2000 lines or 50KB (whichever is hit first). If truncated, full output is saved to a temp file. Optionally provide a timeout in seconds.", Parameters: schema(`{"type":"object","properties":{"command":{"type":"string","description":"PowerShell command to execute"},"timeout":{"type":"number","description":"Timeout in seconds (optional, no default timeout)"}},"required":["command"]}`)}, StructuredOutputSchema: shellOutputSchema, OutputLimits: &durable.OutputLimits{Retain: "tail"}, Execute: powershellTool(options)},
	}}
}

// What programs that call read receive: a text file's shown text, or an
// image as {type, data, mimeType} (upstream readStructuredOutputSchema).
var readOutputSchema = schema(`{"anyOf":[{"type":"string"},{"type":"object","required":["type","data","mimeType"],"properties":{"type":{"const":"image"},"data":{"type":"string"},"mimeType":{"type":"string"}}}]}`)

// What programs that call bash or powershell receive (upstream
// shellStructuredOutputSchema).
var shellOutputSchema = schema(`{"type":"object","required":["output","truncated","exitCode"],"properties":{"output":{"type":"string","description":"Combined stdout and stderr: the retained tail, as the model sees it"},"truncated":{"type":"boolean","description":"Whether earlier output was dropped"},"fullOutputPath":{"type":"string","description":"File with the complete output, when it was spilled"},"exitCode":{"type":"number"}}}`)

// CodingOptions configure the coding tools (upstream createCodingTools).
type CodingOptions struct {
	// Images prepares images the read tool returns: resized, converted, and
	// turned upright (StdImages). Without it, images in an inline format
	// within the default limits pass through as they are.
	Images ImageProcessor
	// Bash configures the bash tool (upstream createBashTool's options).
	Bash ShellToolOptions
}

// Coding returns the coding tools (read, write, edit, bash) without an image
// processor.
func Coding() durable.Extension { return CodingWith(CodingOptions{}) }

// CodingWith returns the coding tools with options.
func CodingWith(options CodingOptions) durable.Extension {
	formats := "jpg, png, gif, webp"
	if options.Images != nil {
		formats += ", bmp"
	}
	return durable.Extension{Name: "coding-tools", Tools: []durable.ToolDefinition{
		{ToolSpec: durable.ToolSpec{Name: "read", Description: "Read the contents of a file. Supports text files and images (" + formats + "). Images are sent as attachments. For text files, output is truncated to 2000 lines or 50KB (whichever is hit first). Use offset/limit for large files. When you need the full file, continue with offset until complete.", Parameters: schema(`{"type":"object","properties":{"path":{"type":"string","description":"Path to the file to read (relative or absolute)"},"offset":{"type":"number","description":"Line number to start reading from (1-indexed)"},"limit":{"type":"number","description":"Maximum number of lines to read"}},"required":["path"]}`)}, StructuredOutputSchema: readOutputSchema, Execute: readToolWith(options.Images)},
		{ToolSpec: durable.ToolSpec{Name: "write", Description: "Write content to a file. Creates the file if it doesn't exist, overwrites if it does. Automatically creates parent directories.", Parameters: schema(`{"type":"object","properties":{"path":{"type":"string","description":"Path to the file to write (relative or absolute)"},"content":{"type":"string","description":"Content to write to the file"}},"required":["path","content"]}`)}, Execute: writeTool},
		{ToolSpec: durable.ToolSpec{Name: "edit", Description: "Edit a single file using exact text replacement. Every edits[].oldText must match a unique, non-overlapping region of the original file. If two changes affect the same block or nearby lines, merge them into one edit instead of emitting overlapping edits. Do not include large unchanged regions just to connect distant changes.", Parameters: schema(`{"type":"object","properties":{"path":{"type":"string","description":"Path to the file to edit (relative or absolute)"},"edits":{"type":"array","items":{"type":"object","properties":{"oldText":{"type":"string","description":"Exact text for one targeted replacement. It must be unique in the original file and must not overlap with any other edits[].oldText in the same call."},"newText":{"type":"string","description":"Replacement text for this targeted edit."}},"required":["oldText","newText"]},"description":"One or more targeted replacements. Each edit is matched against the original file, not incrementally. Do not include overlapping or nested edits. If two changes touch the same block or nearby lines, merge them into one edit instead."}},"required":["path","edits"]}`)}, Execute: editTool},
		{ToolSpec: durable.ToolSpec{Name: "bash", Description: "Execute a bash command in the current working directory. Returns combined stdout and stderr. Output is truncated to last 2000 lines or 50KB (whichever is hit first). If truncated, full output is saved to a temp file. Optionally provide a timeout in seconds.", Parameters: schema(`{"type":"object","properties":{"command":{"type":"string","description":"Bash command to execute"},"timeout":{"type":"number","description":"Timeout in seconds (optional, no default timeout)"}},"required":["command"]}`)}, StructuredOutputSchema: shellOutputSchema, OutputLimits: &durable.OutputLimits{Retain: "tail"}, Execute: bashTool(options.Bash)},
	}}
}

func schema(raw string) json.RawMessage { return json.RawMessage(raw) }

type readArgs struct {
	Path   string   `json:"path"`
	Offset *float64 `json:"offset,omitempty"`
	Limit  *float64 `json:"limit,omitempty"`
}
type writeArgs struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}
type editItem struct {
	OldText string `json:"oldText"`
	NewText string `json:"newText"`
}
type editArgs struct {
	Path  string     `json:"path"`
	Edits []editItem `json:"edits"`
}
type bashArgs struct {
	Command string   `json:"command"`
	Timeout *float64 `json:"timeout,omitempty"`
}

// maxTimeoutSeconds is upstream's largest timeout (a 32-bit millisecond timer).
const maxTimeoutSeconds = 2147483647.0 / 1000

func decodeArgs(raw json.RawMessage, out any) error {
	if len(raw) == 0 {
		return errors.New("missing arguments")
	}
	// Fields the schema does not name are ignored, as upstream's tools read
	// only their own.
	d := json.NewDecoder(bytes.NewReader(raw))
	if err := d.Decode(out); err != nil {
		return fmt.Errorf("invalid arguments: %w", err)
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return errors.New("invalid arguments: trailing JSON")
	}
	return nil
}
func env(rt *durable.ToolRuntime) (durable.ExecutionEnv, error) {
	if rt == nil {
		return nil, errors.New("missing tool runtime")
	}
	e, err := rt.Env()
	if err != nil {
		return nil, err
	}
	if e == nil {
		return nil, errors.New("no execution environment is configured")
	}
	return e, nil
}
func lockMutation(e durable.ExecutionEnv, path string) (func(), error) {
	local, ok := e.(*LocalEnv)
	if !ok {
		return func() {}, nil
	}
	return queueMutation(local, path)
}
func result(content string) (durable.ToolResult, error) {
	return durable.ToolResult{Content: content}, nil
}

func readToolWith(processor ImageProcessor) func(context.Context, json.RawMessage, *durable.ToolRuntime) (durable.ToolResult, error) {
	return func(ctx context.Context, raw json.RawMessage, rt *durable.ToolRuntime) (durable.ToolResult, error) {
		return readTool(ctx, raw, rt, processor)
	}
}

func readTool(ctx context.Context, raw json.RawMessage, rt *durable.ToolRuntime, processor ImageProcessor) (durable.ToolResult, error) {
	var a readArgs
	if err := decodeArgs(raw, &a); err != nil {
		return durable.ToolResult{}, err
	}
	if a.Path == "" {
		return durable.ToolResult{}, errors.New("path must not be empty")
	}
	e, err := env(rt)
	if err != nil {
		return durable.ToolResult{}, err
	}
	// The file read is the resolved path; messages name the path given.
	target, err := resolveReadToolPath(ctx, e, a.Path)
	if err != nil {
		return durable.ToolResult{}, err
	}
	var reader io.Reader
	var reopen func() (io.Reader, func(), error)
	if opener, ok := e.(FileOpener); ok {
		file, err := opener.OpenFile(ctx, target)
		if err != nil {
			return durable.ToolResult{}, err
		}
		defer file.Close()
		reader = file
		reopen = func() (io.Reader, func(), error) {
			again, err := opener.OpenFile(ctx, target)
			if err != nil {
				return nil, nil, err
			}
			return again, func() { _ = again.Close() }, nil
		}
	} else {
		data, err := e.ReadFile(ctx, target)
		if err != nil {
			return durable.ToolResult{}, err
		}
		reader = bytes.NewReader(data)
		reopen = func() (io.Reader, func(), error) { return bytes.NewReader(data), func() {}, nil }
	}
	// An image is told by its first bytes; anything else is text.
	size, known := fileSize(reader)
	buffered := bufio.NewReaderSize(reader, imageHeadBytes)
	head, _ := buffered.Peek(imageHeadBytes)
	if mimeType := detectImageType(head); mimeType != "" {
		var image io.Reader = buffered
		if !known {
			data, err := io.ReadAll(buffered)
			if err != nil {
				return durable.ToolResult{}, err
			}
			size, image = int64(len(data)), bytes.NewReader(data)
		}
		return readImage(image, size, a.Path, mimeType, rt.AcceptsImages(ctx), rt.ImageLimits(ctx), processor)
	}
	return readText(buffered, reopen, a.Path, a.Offset, a.Limit)
}

func writeTool(ctx context.Context, raw json.RawMessage, rt *durable.ToolRuntime) (durable.ToolResult, error) {
	var a writeArgs
	if err := decodeArgs(raw, &a); err != nil {
		return durable.ToolResult{}, err
	}
	if a.Path == "" {
		return durable.ToolResult{}, errors.New("path must not be empty")
	}
	e, err := env(rt)
	if err != nil {
		return durable.ToolResult{}, err
	}
	target, err := resolveToolPath(ctx, e, a.Path)
	if err != nil {
		return durable.ToolResult{}, err
	}
	unlock, err := lockMutation(e, target)
	if err != nil {
		return durable.ToolResult{}, err
	}
	defer unlock()
	if err := e.WriteFile(ctx, target, []byte(a.Content)); err != nil {
		return durable.ToolResult{}, err
	}
	return result("Successfully wrote to " + a.Path)
}

func editTool(ctx context.Context, raw json.RawMessage, rt *durable.ToolRuntime) (durable.ToolResult, error) {
	var a editArgs
	if err := decodeArgs(raw, &a); err != nil {
		return durable.ToolResult{}, err
	}
	if a.Path == "" {
		return durable.ToolResult{}, errors.New("path must not be empty")
	}
	if len(a.Edits) == 0 {
		return durable.ToolResult{}, errors.New("Edit tool input is invalid. edits must contain at least one replacement.")
	}
	e, err := env(rt)
	if err != nil {
		return durable.ToolResult{}, err
	}
	target, err := resolveToolPath(ctx, e, a.Path)
	if err != nil {
		return durable.ToolResult{}, err
	}
	unlock, err := lockMutation(e, target)
	if err != nil {
		return durable.ToolResult{}, err
	}
	defer unlock()
	original, err := e.ReadFile(ctx, target)
	if err != nil {
		return durable.ToolResult{}, editAccessError(ctx, a.Path, err)
	}
	text, bom := string(original), ""
	if strings.HasPrefix(text, "\uFEFF") {
		text, bom = text[len("\uFEFF"):], "\uFEFF"
	}
	ending := detectLineEnding(text)
	edits := make([]textEdit, len(a.Edits))
	for i, edit := range a.Edits {
		edits[i] = textEdit{edit.OldText, edit.NewText}
	}
	base, updated, err := applyEditsToNormalizedContent(normalizeToLF(text), edits, a.Path)
	if err != nil {
		return durable.ToolResult{}, err
	}
	if err := e.WriteFile(ctx, target, []byte(bom+restoreLineEndings(updated, ending))); err != nil {
		return durable.ToolResult{}, editAccessError(ctx, a.Path, err)
	}
	diff, firstChanged := generateDiffString(base, updated, 4)
	details := map[string]any{"diff": diff, "patch": generateUnifiedPatch(a.Path, base, updated, 4)}
	if firstChanged > 0 {
		details["firstChangedLine"] = firstChanged
	}
	data, err := json.Marshal(details)
	if err != nil {
		return durable.ToolResult{}, err
	}
	return durable.ToolResult{Content: fmt.Sprintf("Successfully replaced %d block(s) in %s.", len(a.Edits), a.Path), Details: data}, nil
}

// ShellExecution is a command about to run, which Prepare may change
// (upstream BashExecution): the script, its working directory, and its
// environment.
type ShellExecution struct {
	Command    string
	Cwd        string
	Env        map[string]string
	InheritEnv bool
}

// ShellToolOptions configure the bash and powershell tools (upstream
// BashToolOptions and PowerShellToolOptions).
type ShellToolOptions struct {
	// CommandPrefix runs before each command, on lines of its own.
	CommandPrefix string
	// Prepare may change each command before it runs: its script, working
	// directory, and environment. Working directories and environments need
	// an environment that is a ShellRunner, as LocalEnv is.
	Prepare func(ctx context.Context, execution *ShellExecution, rt *durable.ToolRuntime) error
	// Programs are the PowerShell programs to try in order (powershell
	// only); default pwsh, then powershell. One that cannot start is
	// skipped.
	Programs []string
}

func bashTool(options ShellToolOptions) func(context.Context, json.RawMessage, *durable.ToolRuntime) (durable.ToolResult, error) {
	return func(ctx context.Context, raw json.RawMessage, rt *durable.ToolRuntime) (durable.ToolResult, error) {
		return shellTool(ctx, raw, rt, options, func(execution ShellExecution) []shellForm {
			return []shellForm{{command: execution.Command}}
		})
	}
}

// utf8Output makes PowerShell write UTF-8 whatever the console code page.
const utf8Output = "try { [Console]::OutputEncoding=[System.Text.Encoding]::UTF8 } catch {}"

var powershellArgs = []string{"-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command"}

// powershellTool runs the command through pwsh, else Windows PowerShell,
// started directly with the script as an argument, so no other shell parses
// it.
func powershellTool(options ShellToolOptions) func(context.Context, json.RawMessage, *durable.ToolRuntime) (durable.ToolResult, error) {
	programs := options.Programs
	if len(programs) == 0 {
		programs = []string{"pwsh", "powershell"}
	}
	return func(ctx context.Context, raw json.RawMessage, rt *durable.ToolRuntime) (durable.ToolResult, error) {
		return shellTool(ctx, raw, rt, options, func(execution ShellExecution) []shellForm {
			script := utf8Output + "\n" + execution.Command
			forms := make([]shellForm, len(programs))
			for i, program := range programs {
				forms[i] = shellForm{argv: append(append([]string{program}, powershellArgs...), script)}
			}
			return forms
		})
	}
}

// shellForm is one way to run a command: through the shell, or as argv.
type shellForm struct {
	command string
	argv    []string
}

// shellTool validates a shell call, runs each command form in turn until
// one starts, streaming output to the call's retained output, and shapes
// the result as upstream's: a spill diagnostic, an error result for a
// nonzero exit, and an error for a failure such as a timeout.
func shellTool(ctx context.Context, raw json.RawMessage, rt *durable.ToolRuntime, options ShellToolOptions, forms func(ShellExecution) []shellForm) (durable.ToolResult, error) {
	var a bashArgs
	if err := decodeArgs(raw, &a); err != nil {
		return durable.ToolResult{}, err
	}
	if a.Timeout != nil {
		if math.IsNaN(*a.Timeout) || math.IsInf(*a.Timeout, 0) || *a.Timeout <= 0 {
			return durable.ToolResult{}, errors.New("Invalid timeout: must be a finite number of seconds")
		}
		if *a.Timeout > maxTimeoutSeconds {
			return durable.ToolResult{}, fmt.Errorf("Invalid timeout: maximum is %s seconds", jsNumber(maxTimeoutSeconds))
		}
	}
	e, err := env(rt)
	if err != nil {
		return durable.ToolResult{}, err
	}
	// The command with its prefix, in the environment's working directory,
	// then Prepare (upstream prepareExecution).
	execution := ShellExecution{Command: a.Command, Env: map[string]string{}, InheritEnv: true}
	if options.CommandPrefix != "" {
		execution.Command = options.CommandPrefix + "\n" + a.Command
	}
	if local, ok := e.(*LocalEnv); ok {
		execution.Cwd = local.cwd
	}
	defaultCwd := execution.Cwd
	if options.Prepare != nil {
		if err := options.Prepare(ctx, &execution, rt); err != nil {
			return durable.ToolResult{}, err
		}
	}
	timeout := 0.0
	if a.Timeout != nil {
		timeout = *a.Timeout
	}
	var result ShellExecResult
	if runner, ok := e.(ShellRunner); ok {
		execOptions := ExecOptions{Cwd: execution.Cwd, Env: execution.Env, InheritEnv: &execution.InheritEnv, Timeout: timeout,
			OnOutput: func(text string, _ OutputInfo) error { return rt.Output(ctx, text) },
			Spill:    &SpillOptions{AfterBytes: maxReadBytes, AfterLines: maxReadLines}}
		for _, form := range forms(execution) {
			if form.argv != nil {
				result, err = runner.ExecArgvWith(ctx, form.argv, execOptions)
			} else {
				result, err = runner.ExecWith(ctx, form.command, execOptions)
			}
			// A program that could not start produced no output; the next
			// form may.
			var execErr *ExecutionError
			if err == nil || !errors.As(err, &execErr) || execErr.Code != "spawn_error" {
				break
			}
		}
	} else {
		if execution.Cwd != defaultCwd || len(execution.Env) > 0 || !execution.InheritEnv {
			return durable.ToolResult{}, errors.New("the execution environment cannot run commands in another directory or environment")
		}
		result, err = runPlain(ctx, e, forms(execution), timeout, rt)
	}
	spillPath := result.SpillPath
	var execErr *ExecutionError
	if errors.As(err, &execErr) && execErr.SpillPath != "" {
		spillPath = execErr.SpillPath
	}
	// Reported as the call runs, so a timeout's error result keeps it too
	// (upstream api.diagnostic).
	if spillPath != "" {
		rt.Diagnostic(durable.ToolDiagnostic{Severity: "info", Code: "full_output", Message: "Full output: " + spillPath})
	}
	if err != nil {
		switch {
		case ctx.Err() != nil:
			return durable.ToolResult{}, ctx.Err()
		case execErr != nil && execErr.Code == "timeout":
			return durable.ToolResult{}, fmt.Errorf("Command timed out after %s seconds", jsNumber(timeout))
		case execErr != nil && execErr.Code == "aborted":
			return durable.ToolResult{}, errors.New("Command aborted")
		}
		return durable.ToolResult{}, err
	}
	// The result's content is the output the call kept, which the Harness
	// fills in, with a note after the exit code when the limits dropped some
	// (upstream). Programs get the retained output with the exit code.
	retained, truncated := rt.RetainedOutput(ctx)
	output := map[string]any{"output": retained, "truncated": truncated, "exitCode": result.ExitCode}
	if spillPath != "" {
		output["fullOutputPath"] = spillPath
	}
	structured, err := json.Marshal(output)
	if err != nil {
		return durable.ToolResult{}, err
	}
	if result.ExitCode != 0 {
		exited := durable.ToolDiagnostic{Severity: "error", Code: "exit_code", Message: fmt.Sprintf("Command exited with code %d", result.ExitCode)}
		return durable.ToolResult{StructuredOutput: structured, IsError: true, Diagnostics: []durable.ToolDiagnostic{exited}}, nil
	}
	return durable.ToolResult{StructuredOutput: structured}, nil
}

// runPlain runs a command through an environment that is no ShellRunner:
// its Exec, or its ExecArgv for argv forms, with the timeout as a context
// deadline and without a spill.
func runPlain(ctx context.Context, e durable.ExecutionEnv, forms []shellForm, timeout float64, rt *durable.ToolRuntime) (ShellExecResult, error) {
	runCtx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, time.Duration(timeout*float64(time.Second)))
		defer cancel()
	}
	streamed := false
	onOutput := func(text string) error {
		streamed = true
		return rt.Output(ctx, text)
	}
	var res durable.ExecResult
	var err error
	for _, form := range forms {
		if form.argv != nil {
			runner, ok := e.(ArgvExecutor)
			if !ok {
				return ShellExecResult{}, errors.New("the execution environment cannot run programs directly")
			}
			res, err = runner.ExecArgv(runCtx, form.argv, onOutput)
			if errors.Is(err, ErrSpawn) {
				continue // the next program may start
			}
		} else {
			res, err = e.Exec(runCtx, form.command, onOutput)
		}
		break
	}
	// An environment that returns its output without streaming it.
	if !streamed && res.Output != "" {
		if outputErr := rt.Output(ctx, res.Output); outputErr != nil && err == nil {
			err = outputErr
		}
	}
	if err != nil && ctx.Err() == nil && runCtx.Err() != nil {
		return ShellExecResult{}, &ExecutionError{Code: "timeout", Message: "timeout"}
	}
	return ShellExecResult{ExitCode: res.ExitCode}, err
}

// jsNumber prints a number as JavaScript does in a template string.
func jsNumber(n float64) string { return jsnum.String(n) }

// editAccessError is upstream's message for a file the edit tool cannot read
// or write: the environment's file error code, or that the path is not a
// file. The cause stays reachable with errors.Is and errors.As.
func editAccessError(ctx context.Context, path string, err error) error {
	if ctx.Err() != nil {
		return err
	}
	code := fileErrorCode(err)
	if code == "is_directory" {
		return &fileAccessError{message: fmt.Sprintf("Could not edit file: %s. Path is not a file.", path), cause: err}
	}
	return &fileAccessError{message: fmt.Sprintf("Could not edit file: %s. Error code: %s.", path, code), cause: err}
}

type fileAccessError struct {
	message string
	cause   error
}

func (e *fileAccessError) Error() string { return e.message }
func (e *fileAccessError) Unwrap() error { return e.cause }

// fileErrorCode is upstream's FileErrorCode for a file system error.
func fileErrorCode(err error) string {
	var fileError *FileError
	if errors.As(err, &fileError) {
		return fileError.Code
	}
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "aborted"
	case errors.Is(err, fs.ErrNotExist):
		return "not_found"
	case errors.Is(err, fs.ErrPermission):
		return "permission_denied"
	case errors.Is(err, syscall.ENOTDIR):
		return "not_directory"
	case errors.Is(err, syscall.EISDIR):
		return "is_directory"
	case errors.Is(err, fs.ErrInvalid):
		return "invalid"
	}
	return "unknown"
}
