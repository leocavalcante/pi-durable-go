package tools

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Upstream's watch conformance cases (testing/env-conformance.ts), on a
// local environment in each mode.

// covers reports whether a change reports path: an overflow, or a reported
// path at or above it.
func covers(change FileChange, path string) bool {
	if change.Kind == "overflow" {
		return true
	}
	if change.Kind != "paths" {
		return false
	}
	for _, reported := range change.Paths {
		if path == reported || strings.HasPrefix(path, reported+"/") {
			return true
		}
	}
	return false
}

type watchRun struct {
	t       *testing.T
	env     *LocalEnv
	mu      sync.Mutex
	changes []FileChange
}

func (w *watchRun) all() []FileChange {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]FileChange(nil), w.changes...)
}

// expectChange waits up to three seconds, after change, for a change that
// reports path.
func (w *watchRun) expectChange(path string, change func()) {
	w.t.Helper()
	target := w.env.resolve(path)
	from := len(w.all())
	change()
	deadline := time.Now().Add(3 * time.Second)
	for {
		for _, entry := range w.all()[from:] {
			if covers(entry, target) {
				return
			}
		}
		if time.Now().After(deadline) {
			w.t.Fatalf("no change reported %s; got %+v", target, w.all()[from:])
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// watching watches targets while run changes files, in each mode.
func watching(t *testing.T, setup func(env *LocalEnv), targets []WatchTarget, run func(w *watchRun)) {
	for _, mode := range []string{"native", "polling"} {
		t.Run(mode, func(t *testing.T) {
			if mode == "native" && !nativeWatching {
				t.Skip("no native watching here")
			}
			dir, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			env, err := NewLocalEnvWith(dir, LocalEnvOptions{Watch: WatchOptions{Mode: mode, PollInterval: 100 * time.Millisecond}})
			if err != nil {
				t.Fatal(err)
			}
			if setup != nil {
				setup(env)
			}
			w := &watchRun{t: t, env: env}
			watcher, err := env.Watch(context.Background(), targets, func(change FileChange) {
				w.mu.Lock()
				w.changes = append(w.changes, change)
				w.mu.Unlock()
			})
			if err != nil {
				t.Fatal(err)
			}
			defer watcher.Stop(context.Background())
			if watcher.Mode() != mode {
				t.Fatalf("mode %s, want %s", watcher.Mode(), mode)
			}
			run(w)
		})
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func writeText(t *testing.T, env *LocalEnv, name, text string) {
	t.Helper()
	must(t, env.WriteFile(context.Background(), name, []byte(text)))
}

func TestWatchReportsAMissingFilesCreationChangesReplacementAndRemoval(t *testing.T) {
	watching(t, nil, []WatchTarget{{Path: "AGENTS.md"}}, func(w *watchRun) {
		w.expectChange("AGENTS.md", func() { writeText(t, w.env, "AGENTS.md", "one") })
		w.expectChange("AGENTS.md", func() { writeText(t, w.env, "AGENTS.md", "two!") })
		// Editors replace a file by renaming a new one over it.
		w.expectChange("AGENTS.md", func() {
			writeText(t, w.env, "AGENTS.md.tmp", "three")
			must(t, w.env.RenameFile(context.Background(), "AGENTS.md.tmp", "AGENTS.md"))
		})
		w.expectChange("AGENTS.md", func() { writeText(t, w.env, "AGENTS.md", "four") })
		w.expectChange("AGENTS.md", func() { must(t, w.env.Remove(context.Background(), "AGENTS.md", false, false)) })
	})
}

func TestWatchReportsAMissingTargetWhoseAncestorsAreCreated(t *testing.T) {
	watching(t, nil, []WatchTarget{{Path: "a/b/c/AGENTS.md"}}, func(w *watchRun) {
		w.expectChange("a/b/c/AGENTS.md", func() { writeText(t, w.env, "a/b/c/AGENTS.md", "x") })
	})
}

func TestWatchFollowsDirectoriesCreatedTogetherWithTheirContents(t *testing.T) {
	setup := func(env *LocalEnv) { must(t, env.CreateDir(context.Background(), "skills", true)) }
	watching(t, setup, []WatchTarget{{Path: "skills", Recursive: true}}, func(w *watchRun) {
		// Written before any watcher on the new directories can exist.
		w.expectChange("skills/a/b/SKILL.md", func() { writeText(t, w.env, "skills/a/b/SKILL.md", "one") })
		w.expectChange("skills/a/b/SKILL.md", func() { writeText(t, w.env, "skills/a/b/SKILL.md", "two!") })
		w.expectChange("skills/a/b/c/SKILL.md", func() { writeText(t, w.env, "skills/a/b/c/SKILL.md", "deeper") })
	})
}

func TestWatchKeepsWatchingAPathWhoseParentIsRenamedAndRecreated(t *testing.T) {
	setup := func(env *LocalEnv) { writeText(t, env, "proj/.pi/skills/x.md", "x") }
	watching(t, setup, []WatchTarget{{Path: "proj/.pi/skills", Recursive: true}}, func(w *watchRun) {
		w.expectChange("proj/.pi/skills", func() { must(t, w.env.RenameFile(context.Background(), "proj/.pi", "proj/old")) })
		w.expectChange("proj/.pi/skills/y.md", func() { writeText(t, w.env, "proj/.pi/skills/y.md", "y") })
		w.expectChange("proj/.pi/skills/y.md", func() { writeText(t, w.env, "proj/.pi/skills/y.md", "yy") })
	})
}

func TestWatchSkipsExcludedEntriesAndReportsARenameOutOfThem(t *testing.T) {
	setup := func(env *LocalEnv) { must(t, env.CreateDir(context.Background(), "skills", true)) }
	targets := []WatchTarget{{Path: "skills", Recursive: true, ExcludeHidden: true, ExcludeNames: []string{"node_modules"}}}
	watching(t, setup, targets, func(w *watchRun) {
		writeText(t, w.env, "skills/node_modules/dep/SKILL.md", "dep")
		writeText(t, w.env, "skills/.SKILL.md.tmp", "draft")
		w.expectChange("skills/SKILL.md", func() {
			must(t, w.env.RenameFile(context.Background(), "skills/.SKILL.md.tmp", "skills/SKILL.md"))
		})
		hidden := []string{w.env.resolve("skills/node_modules"), w.env.resolve("skills/.SKILL.md.tmp")}
		for _, change := range w.all() {
			for _, path := range change.Paths {
				for _, excluded := range hidden {
					if strings.HasPrefix(path, excluded) {
						t.Fatalf("excluded %s", path)
					}
				}
			}
		}
	})
}

func TestWatchKeepsRecursiveCoverageWhereANonRecursiveTargetOverlaps(t *testing.T) {
	setup := func(env *LocalEnv) { writeText(t, env, "skills/a/one.md", "one") }
	watching(t, setup, []WatchTarget{{Path: "skills"}, {Path: "skills", Recursive: true}}, func(w *watchRun) {
		w.expectChange("skills/a/two.md", func() { writeText(t, w.env, "skills/a/two.md", "two") })
	})
}

func TestWatchFollowsADirectoryReplacedAtTheSamePath(t *testing.T) {
	setup := func(env *LocalEnv) { writeText(t, env, "skills/a/x.md", "x") }
	watching(t, setup, []WatchTarget{{Path: "skills", Recursive: true}}, func(w *watchRun) {
		w.expectChange("skills/a", func() {
			must(t, w.env.RenameFile(context.Background(), "skills/a", "skills-old"))
			must(t, w.env.CreateDir(context.Background(), "skills/a", true))
		})
		w.expectChange("skills/a/y.md", func() { writeText(t, w.env, "skills/a/y.md", "y") })
		w.expectChange("skills/a/y.md", func() { writeText(t, w.env, "skills/a/y.md", "yy") })
	})
}

func TestWatchStopsReportingOnceStopped(t *testing.T) {
	env, err := NewLocalEnv(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var changes []FileChange
	watcher, err := env.Watch(context.Background(), []WatchTarget{{Path: "file.txt"}}, func(change FileChange) {
		mu.Lock()
		changes = append(changes, change)
		mu.Unlock()
	})
	if err != nil {
		t.Fatal(err)
	}
	if mode := watcher.Mode(); mode != "native" && mode != "polling" {
		t.Fatalf("mode %q", mode)
	}
	must(t, watcher.Stop(context.Background()))
	must(t, watcher.Stop(context.Background()))
	writeText(t, env, "file.txt", "x")
	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(changes) != 0 {
		t.Fatalf("changes after stop: %+v", changes)
	}
}

func TestWatchReportsChangesToTheFileAWatchedSymbolicLinkPointsTo(t *testing.T) {
	setup := func(env *LocalEnv) {
		writeText(t, env, "data/real.md", "one")
		must(t, env.CreateDir(context.Background(), "config", true))
		must(t, os.Symlink("../data/real.md", env.resolve("config/AGENTS.md")))
	}
	watching(t, setup, []WatchTarget{{Path: "config/AGENTS.md"}}, func(w *watchRun) {
		w.expectChange("config/AGENTS.md", func() { writeText(t, w.env, "data/real.md", "two!") })
	})
}

// A watcher over too many directories fails as upstream's does.
func TestWatchRefusesTooManyDirectories(t *testing.T) {
	env, err := NewLocalEnvWith(t.TempDir(), LocalEnvOptions{Watch: WatchOptions{MaxDirectories: 2}})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"a/x", "a/y", "a/z"} {
		must(t, env.CreateDir(context.Background(), name, true))
	}
	_, err = env.Watch(context.Background(), []WatchTarget{{Path: "a", Recursive: true}}, func(FileChange) {})
	if fileError, ok := err.(*FileError); !ok || fileError.Code != "invalid" || fileError.Message != "Watched paths exceed 2 directories" {
		t.Fatalf("error %v", err)
	}
}
