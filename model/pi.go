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

var _ durable.Model = Pi{}

func (p Pi) Generate(ctx context.Context, req durable.ModelRequest, onDelta func(string) error) (durable.ModelResponse, error) {
	if err := ctx.Err(); err != nil {
		return durable.ModelResponse{}, err
	}
	if req.Model.Provider == "" || req.Model.ID == "" {
		return durable.ModelResponse{}, errors.New("pi: model provider and ID are required")
	}
	body, err := json.Marshal(struct {
		Request    durable.ModelRequest `json:"request"`
		PackageDir string               `json:"packageDir,omitempty"`
		AgentDir   string               `json:"agentDir,omitempty"`
	}{req, p.packageDir(), p.AgentDir})
	if err != nil {
		return durable.ModelResponse{}, errors.New("pi: cannot encode model request")
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
		return durable.ModelResponse{}, errors.New("pi: cannot create request pipe")
	}
	defer input.Close()
	output, err := cmd.StdoutPipe()
	if err != nil {
		return durable.ModelResponse{}, errors.New("pi: cannot create response pipe")
	}
	// SDK/provider diagnostics can contain credentials or response bodies. Never
	// publish stderr or raw child errors into durable task failure records.
	cmd.Stderr = io.Discard
	cmd.WaitDelay = time.Second
	if err := cmd.Start(); err != nil {
		return durable.ModelResponse{}, errors.New("pi: cannot start Node; configure NodePath or install Node.js 22.19+")
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
		return durable.ModelResponse{}, errors.New("pi: SDK process failed")
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
		}
		if response != nil || json.Unmarshal(scanner.Bytes(), &event) != nil {
			return durable.ModelResponse{}, errors.New("pi: invalid SDK stream")
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
				return durable.ModelResponse{}, errors.New("pi: invalid SDK result")
			}
			response = event.Response
		case "error":
			return durable.ModelResponse{}, piBridgeError(event.Code)
		default:
			return durable.ModelResponse{}, errors.New("pi: invalid SDK event")
		}
	}
	if err := scanner.Err(); err != nil {
		if errors.Is(err, errResponseTooLarge) {
			return durable.ModelResponse{}, errResponseTooLarge
		}
		return durable.ModelResponse{}, errors.New("pi: SDK stream read failed")
	}
	if response == nil {
		return durable.ModelResponse{}, errors.New("pi: SDK stream ended without a result")
	}
	return *response, nil
}

func piBridgeError(code string) error {
	// Only locally defined codes cross the boundary. Never return upstream error
	// messages, including JSON parser errors that can quote secrets.
	switch code {
	case "sdk":
		return errors.New("pi: SDK unavailable; install pi-coding-agent 1.5.2 and configure PackageDir")
	case "config":
		return errors.New("pi: invalid Pi model configuration; check models.json privately")
	case "model":
		return errors.New("pi: model not found; check the provider/ID and Pi models.json")
	case "request":
		return errors.New("pi: unsupported or invalid model request")
	case "length":
		return errors.New("pi: model output reached the token limit")
	default:
		return errors.New("pi: provider request failed; check credentials with Pi /login and provider configuration")
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
