// Package piinstall finds Node and an installed Pi coding agent, for the
// packages that delegate to Pi's own JavaScript (model.Pi and the code mode
// sandbox).
package piinstall

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// PackageName is the npm package of the Pi coding agent.
const PackageName = "@earendil-works/pi-coding-agent"

// NodePath is configured when set, else node on PATH, else Pi's managed Node
// installation, else "node", whose start fails with the caller's setup error.
func NodePath(configured string) string {
	if configured != "" {
		return configured
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
	return "node"
}

// PackageDir is the configured pi-coding-agent package directory when set,
// else PI_PACKAGE_DIR, a managed Pi install (PI_MANAGED_INSTALL_ROOT, or the
// one pi on PATH belongs to), or the npm package pi on PATH links to. Empty
// when none is found.
func PackageDir(configured string) string {
	if configured != "" {
		return configured
	}
	if dir := os.Getenv("PI_PACKAGE_DIR"); dir != "" {
		return dir
	}
	// Pi's managed launcher is not a symlink to its npm entry point. Its
	// current-version file selects a release without executing the launcher.
	if root := os.Getenv("PI_MANAGED_INSTALL_ROOT"); root != "" {
		if dir := Managed(root); dir != "" {
			return dir
		}
	}
	if bin, err := exec.LookPath("pi"); err == nil {
		if bin, err = filepath.EvalSymlinks(bin); err == nil {
			if dir := Managed(filepath.Join(filepath.Dir(bin), "..", "install")); dir != "" {
				return dir
			}
			// npm links pi to dist/cli.js or dist/bundle/cli.js.
			dir := filepath.Dir(bin)
			for range 3 {
				if IsPackage(dir) {
					return dir
				}
				dir = filepath.Dir(dir)
			}
		}
	}
	return ""
}

// Managed is the coding agent of a managed Pi install's current release, or
// empty.
func Managed(root string) string {
	data, err := os.ReadFile(filepath.Join(root, "current-version"))
	if err != nil {
		return ""
	}
	version := strings.TrimSpace(string(data))
	if version == "" || version == "." || version == ".." || strings.ContainsAny(version, `/\\`) {
		return ""
	}
	dir := filepath.Join(root, "releases", version, "node_modules", filepath.FromSlash(PackageName))
	if IsPackage(dir) {
		return dir
	}
	return ""
}

// IsPackage reports whether dir holds the pi-coding-agent package.
func IsPackage(dir string) bool {
	data, err := os.ReadFile(filepath.Join(dir, "package.json"))
	if err != nil {
		return false
	}
	var pkg struct{ Name string }
	return json.Unmarshal(bytes.TrimPrefix(data, []byte{0xef, 0xbb, 0xbf}), &pkg) == nil && pkg.Name == PackageName
}
