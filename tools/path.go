package tools

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"

	durable "github.com/leocavalcante/pi-durable-go"
	"golang.org/x/text/unicode/norm"
)

// PathResolver is an environment that resolves paths and tells whether one
// exists (upstream ExecutionEnv absolutePath and exists), which the coding
// tools use to resolve the paths they are given. LocalEnv is one.
type PathResolver interface {
	AbsolutePath(ctx context.Context, path string) (string, error)
	Exists(ctx context.Context, path string) (bool, error)
}

var unicodeSpaces = regexp.MustCompile("[\u00A0\u2000-\u200A\u202F\u205F\u3000]")

// normalizeToolPath is upstream's: Unicode spaces become spaces, and a
// leading @ (a mention) is dropped.
func normalizeToolPath(path string) string {
	return strings.TrimPrefix(unicodeSpaces.ReplaceAllString(path, " "), "@")
}

// resolveToolPath is the path a tool reads or writes for the path it was
// given (upstream resolveToolPath).
func resolveToolPath(ctx context.Context, e durable.ExecutionEnv, path string) (string, error) {
	path = normalizeToolPath(path)
	if resolver, ok := e.(PathResolver); ok {
		return resolver.AbsolutePath(ctx, path)
	}
	return path, nil
}

var amPM = regexp.MustCompile(`(?i) (AM|PM)\.`)

// resolveReadToolPath is upstream's resolveReadToolPath: the resolved path,
// or the first that exists of its variants a model's typing often misses:
// macOS screenshot names (a narrow no-break space before AM or PM),
// decomposed accents, and curly apostrophes.
func resolveReadToolPath(ctx context.Context, e durable.ExecutionEnv, path string) (string, error) {
	resolved, err := resolveToolPath(ctx, e, path)
	if err != nil {
		return "", err
	}
	resolver, ok := e.(PathResolver)
	if !ok {
		return resolved, nil
	}
	decomposed := norm.NFD.String(resolved)
	seen := map[string]bool{}
	for _, variant := range []string{resolved, amPM.ReplaceAllString(resolved, "\u202F${1}."), decomposed,
		strings.ReplaceAll(resolved, "'", "\u2019"), strings.ReplaceAll(decomposed, "'", "\u2019")} {
		if seen[variant] {
			continue
		}
		seen[variant] = true
		exists, err := resolver.Exists(ctx, variant)
		if err != nil {
			return "", err
		}
		if exists {
			return variant, nil
		}
	}
	return resolved, nil
}

// resolveLocalPath is upstream's Node environment resolvePath: ~ and ~/
// name the home directory, a file URL its path, and a relative path is
// based at cwd.
func resolveLocalPath(cwd, path string) string {
	switch {
	case path == "~":
		if home, err := os.UserHomeDir(); err == nil {
			path = home
		}
	case strings.HasPrefix(path, "~/") || runtime.GOOS == "windows" && strings.HasPrefix(path, `~\`):
		if home, err := os.UserHomeDir(); err == nil {
			path = filepath.Join(home, path[2:])
		}
	case strings.HasPrefix(path, "file://"):
		// A malformed URL stays an ordinary path.
		if u, err := url.Parse(path); err == nil && (u.Host == "" || u.Host == "localhost") && !strings.Contains(strings.ToLower(u.RawPath), "%2f") {
			path = filepath.FromSlash(u.Path)
		}
	}
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	return filepath.Join(cwd, path)
}
