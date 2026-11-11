package model

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	durable "github.com/leocavalcante/pi-durable-go"
)

//go:embed pi_bridge.mjs
var piBridge string

const piPackageName = "@earendil-works/pi-coding-agent"

// Pi delegates model requests to the installed Pi coding-agent SDK. Pi owns
// credential resolution/refresh and provider conversion; Go owns the transcript,
// scheduling, and all tool execution. No AgentSession or project extensions load.
// The SDK baseline is 1.5.2 (ModelRuntime), separate from the Durable reference.
// A fresh Node process is started per request; Pi's auth.json locking coordinates
// credential refresh with other Pi processes. Do not mutate options during use.
// See README.md for installation, discovery, and security considerations.
type Pi struct {
	// NodePath defaults to node on PATH, then Pi's managed Node installation.
	NodePath string
	// PackageDir selects a trusted installed pi-coding-agent package directory.
	// Empty discovers PI_PACKAGE_DIR, a managed Pi install, or pi on PATH, then
	// falls back to Node's package resolution in the working directory.
	PackageDir string
	// AgentDir selects auth.json and models.json together. Empty uses Pi's
	// default (~/.pi/agent, or PI_CODING_AGENT_DIR). No project settings load.
	AgentDir string
	// MaxResponseBytes caps the complete bridge output, including native replay
	// data and streamed deltas. Defaults to 8 MiB.
	MaxResponseBytes int64
}

var (
	_ durable.Model              = Pi{}
	_ durable.ContextWindowModel = Pi{}
	_ durable.ImageInputModel    = Pi{}
	_ durable.ImageLimitsModel   = Pi{}
)

// ImageLimits reports the model's image resize limits from Pi's catalog;
// zero fields where it sets none.
func (p Pi) ImageLimits(ref durable.ModelRef) durable.ImageLimits {
	entry, _ := p.entry(ref)
	return entry.limits
}

// catalogEntry is what the bridge reads of a model from Pi's catalog.
type catalogEntry struct {
	window int
	images bool
	limits durable.ImageLimits
}

// catalog caches catalog entries per installation and model.
var catalog sync.Map

// entry reads the model's catalog entry once per package, agent directory,
// and model through the bridge, without a provider call.
func (p Pi) entry(ref durable.ModelRef) (catalogEntry, bool) {
	key := p.packageDir() + "\x00" + p.AgentDir + "\x00" + ref.Provider + "\x00" + ref.ID
	if cached, ok := catalog.Load(key); ok {
		return cached.(catalogEntry), true
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	entry, err := p.describe(ctx, ref)
	if err != nil {
		return catalogEntry{}, false // not cached: a later call may succeed
	}
	catalog.Store(key, entry)
	return entry, true
}

// ContextWindow reports the model's context window from Pi's catalog; 0 when
// unknown, which disables threshold compaction.
func (p Pi) ContextWindow(ref durable.ModelRef) int {
	entry, _ := p.entry(ref)
	return entry.window
}

// AcceptsImages reports whether Pi's catalog lists image input for the
// model; true when unknown, as upstream assumes.
func (p Pi) AcceptsImages(ref durable.ModelRef) bool {
	entry, ok := p.entry(ref)
	return !ok || entry.images
}

func (p Pi) describe(ctx context.Context, ref durable.ModelRef) (catalogEntry, error) {
	body, err := json.Marshal(struct {
		Request    durable.ModelRequest `json:"request"`
		PackageDir string               `json:"packageDir,omitempty"`
		AgentDir   string               `json:"agentDir,omitempty"`
		Describe   bool                 `json:"describe"`
	}{durable.ModelRequest{Model: ref}, p.packageDir(), p.AgentDir, true})
	if err != nil {
		return catalogEntry{}, err
	}
	cmd := exec.CommandContext(ctx, p.nodePath(), "--input-type=module", "--eval", piBridge)
	cmd.Stdin = bytes.NewReader(append(body, '\n'))
	cmd.Stderr = io.Discard
	out, err := cmd.Output()
	if err != nil {
		return catalogEntry{}, err
	}
	var event struct {
		Type          string              `json:"type"`
		ContextWindow int                 `json:"contextWindow"`
		Images        *bool               `json:"images"`
		ImageLimits   durable.ImageLimits `json:"imageLimits"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(out), &event); err != nil || event.Type != "model" {
		return catalogEntry{}, errors.New("pi: cannot describe model")
	}
	return catalogEntry{window: event.ContextWindow, images: event.Images == nil || *event.Images, limits: event.ImageLimits}, nil
}

func (p Pi) Generate(ctx context.Context, req durable.ModelRequest, onDelta func(string) error) (durable.ModelResponse, error) {
	if err := ctx.Err(); err != nil {
		return durable.ModelResponse{}, err
	}
	if req.Model.Provider == "" || req.Model.ID == "" {
		return durable.ModelResponse{}, sanitizedError("pi: model provider and ID are required", false)
	}
	body, err := json.Marshal(struct {
		Request    durable.ModelRequest `json:"request"`
		PackageDir string               `json:"packageDir,omitempty"`
		AgentDir   string               `json:"agentDir,omitempty"`
	}{req, p.packageDir(), p.AgentDir})
	if err != nil {
		return durable.ModelResponse{}, sanitizedError("pi: cannot encode model request", false)
	}
	node := p.nodePath()
	// Use a cancellation pipe rather than killing Node immediately: OAuth refresh
	// may rotate credentials before its write completes. Give the SDK a bounded
	// grace period to settle those writes, then force termination if necessary.
	processCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmd := exec.CommandContext(processCtx, node, "--input-type=module", "--eval", piBridge)
	input, err := cmd.StdinPipe()
	if err != nil {
		return durable.ModelResponse{}, sanitizedError("pi: cannot create request pipe", false)
	}
	defer input.Close()
	output, err := cmd.StdoutPipe()
	if err != nil {
		return durable.ModelResponse{}, sanitizedError("pi: cannot create response pipe", false)
	}
	// SDK/provider diagnostics can contain credentials or response bodies. Never
	// publish stderr or raw child errors into durable task failure records.
	cmd.Stderr = io.Discard
	cmd.WaitDelay = time.Second
	if err := cmd.Start(); err != nil {
		return durable.ModelResponse{}, sanitizedError("pi: cannot start Node; configure NodePath or install Node.js 22.19+", false)
	}
	finished := make(chan struct{})
	stop := make(chan struct{})
	go func() {
		defer close(finished)
		select {
		case <-ctx.Done():
			input.Close() // EOF aborts generation in the bridge, after initial input.
			timer := time.NewTimer(3 * time.Second)
			defer timer.Stop()
			select {
			case <-stop:
			case <-timer.C:
				cancel()
				// Descendants of a broken SDK might retain stdout after Node
				// dies. Unblock the reader even in that case.
				output.Close()
			}
		case <-stop:
		}
	}()
	// Stdout is consumed before Wait, and Wait is always called, including on
	// callback errors, malformed output, and size-limit violations.
	_, writeErr := input.Write(append(body, '\n'))
	maxBytes := p.MaxResponseBytes
	if maxBytes <= 0 {
		maxBytes = defaultMaxResponseBytes
	}
	var response durable.ModelResponse
	var readErr error
	if writeErr == nil {
		response, readErr = readPiOutput(&limitedReader{r: output, remaining: maxBytes}, onDelta)
	}
	if writeErr != nil || readErr != nil {
		cancel()
	}
	waitErr := cmd.Wait()
	close(stop)
	<-finished
	if err := ctx.Err(); err != nil {
		return durable.ModelResponse{}, err
	}
	if readErr != nil {
		return durable.ModelResponse{}, readErr
	}
	if writeErr != nil || waitErr != nil {
		return durable.ModelResponse{}, sanitizedError("pi: SDK process failed", false)
	}
	return response, nil
}

func readPiOutput(r io.Reader, onDelta func(string) error) (durable.ModelResponse, error) {
	scanner := bufio.NewScanner(r)
	// The final record includes the assistant's native replay data, which can
	// exceed a single provider SSE line. The reader still caps total output.
	scanner.Buffer(make([]byte, 4096), int(^uint(0)>>1))
	var response *durable.ModelResponse
	for scanner.Scan() {
		var event struct {
			Type     string                 `json:"type"`
			Delta    string                 `json:"delta"`
			Response *durable.ModelResponse `json:"response"`
			Code     string                 `json:"code"`
			// pi-ai's classification of the provider error, computed in Node.
			Retryable bool `json:"retryable"`
			Overflow  bool `json:"overflow"`
		}
		if response != nil || json.Unmarshal(scanner.Bytes(), &event) != nil {
			return durable.ModelResponse{}, sanitizedError("pi: invalid SDK stream", false)
		}
		switch event.Type {
		case "delta":
			if onDelta != nil {
				if err := onDelta(event.Delta); err != nil {
					return durable.ModelResponse{}, err
				}
			}
		case "result":
			if event.Response == nil {
				return durable.ModelResponse{}, sanitizedError("pi: invalid SDK result", false)
			}
			response = event.Response
		case "error":
			return durable.ModelResponse{}, piBridgeError(event.Code, event.Retryable, event.Overflow)
		default:
			return durable.ModelResponse{}, sanitizedError("pi: invalid SDK event", false)
		}
	}
	if err := scanner.Err(); err != nil {
		if errors.Is(err, errResponseTooLarge) {
			return durable.ModelResponse{}, errResponseTooLarge
		}
		return durable.ModelResponse{}, sanitizedError("pi: SDK stream read failed", false)
	}
	if response == nil {
		return durable.ModelResponse{}, sanitizedError("pi: SDK stream ended without a result", false)
	}
	return *response, nil
}

func piBridgeError(code string, retryable, overflow bool) error {
	// Only locally defined codes cross the boundary. Never return upstream error
	// messages, including JSON parser errors that can quote secrets. A provider
	// error carries pi-ai's own retry and overflow classification instead.
	switch code {
	case "sdk":
		return sanitizedError("pi: SDK unavailable; install pi-coding-agent 1.5.2 and configure PackageDir", false)
	case "config":
		return sanitizedError("pi: invalid Pi model configuration; check models.json privately", false)
	case "model":
		return sanitizedError("pi: model not found; check the provider/ID and Pi models.json", false)
	case "request":
		return sanitizedError("pi: unsupported or invalid model request", false)
	case "length":
		return sanitizedError("pi: model output reached the token limit", false)
	default:
		return &providerError{message: "pi: provider request failed; check credentials with Pi /login and provider configuration",
			retryable: retryable, overflow: overflow}
	}
}

func (p Pi) nodePath() string {
	if p.NodePath != "" {
		return p.NodePath
	}
	if node, err := exec.LookPath("node"); err == nil {
		return node
	}
	data := os.Getenv("XDG_DATA_HOME")
	if data == "" {
		if home, err := os.UserHomeDir(); err == nil {
			data = filepath.Join(home, ".local", "share")
		}
	}
	if data != "" {
		node := filepath.Join(data, "pi-node", "current", "bin", "node")
		if info, err := os.Stat(node); err == nil && !info.IsDir() && info.Mode()&0111 != 0 {
			return node
		}
	}
	return "node" // Start returns the sanitized setup error.
}

func (p Pi) packageDir() string {
	if p.PackageDir != "" {
		return p.PackageDir
	}
	if dir := os.Getenv("PI_PACKAGE_DIR"); dir != "" {
		return dir
	}
	// Pi's managed launcher is not a symlink to its npm entry point. Its
	// current-version file selects a release without executing the launcher.
	if root := os.Getenv("PI_MANAGED_INSTALL_ROOT"); root != "" {
		if dir := managedPiPackage(root); dir != "" {
			return dir
		}
	}
	if bin, err := exec.LookPath("pi"); err == nil {
		if bin, err = filepath.EvalSymlinks(bin); err == nil {
			if dir := managedPiPackage(filepath.Join(filepath.Dir(bin), "..", "install")); dir != "" {
				return dir
			}
			// npm links pi to dist/cli.js or dist/bundle/cli.js.
			dir := filepath.Dir(bin)
			for range 3 {
				if isPiPackage(dir) {
					return dir
				}
				dir = filepath.Dir(dir)
			}
		}
	}
	return ""
}

func managedPiPackage(root string) string {
	data, err := os.ReadFile(filepath.Join(root, "current-version"))
	if err != nil {
		return ""
	}
	version := strings.TrimSpace(string(data))
	if version == "" || version == "." || version == ".." || strings.ContainsAny(version, `/\\`) {
		return ""
	}
	dir := filepath.Join(root, "releases", version, "node_modules", filepath.FromSlash(piPackageName))
	if isPiPackage(dir) {
		return dir
	}
	return ""
}

func isPiPackage(dir string) bool {
	data, err := os.ReadFile(filepath.Join(dir, "package.json"))
	if err != nil {
		return false
	}
	var pkg struct{ Name string }
	return json.Unmarshal(bytes.TrimPrefix(data, []byte{0xef, 0xbb, 0xbf}), &pkg) == nil && pkg.Name == piPackageName
}
