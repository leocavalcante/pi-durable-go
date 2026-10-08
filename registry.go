package durable

import (
	"fmt"
	"regexp"
	"sync"
)

var sectionNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)

// Registry is a process-local, dynamically replaceable extension registry.
type Registry struct {
	mu         sync.RWMutex
	extensions []Extension
	listeners  map[uint64]func()
	next       uint64
}

// RegistrySnapshot is an immutable ordered view of installed extensions.
type RegistrySnapshot struct {
	extensions []Extension
	tasks      map[string]TaskDefinition
}

func NewRegistry() *Registry { return &Registry{listeners: make(map[uint64]func())} }

func (r *Registry) Install(ext Extension) error {
	if r == nil || ext.Name == "" {
		return fmt.Errorf("%w: extension name is required", ErrInvalid)
	}
	ext = cloneExtension(ext)
	if err := validateExtension(ext); err != nil {
		return err
	}
	r.mu.Lock()
	next := append([]Extension(nil), r.extensions...)
	index := -1
	for i := range next {
		if next[i].Name == ext.Name {
			index = i
			break
		}
	}
	if index < 0 {
		next = append(next, ext)
	} else {
		next[index] = ext
	}
	if _, err := snapshotOf(next); err != nil {
		r.mu.Unlock()
		return err
	}
	r.extensions = next
	listeners := make([]func(), 0, len(r.listeners))
	for _, fn := range r.listeners {
		listeners = append(listeners, fn)
	}
	r.mu.Unlock()
	for _, fn := range listeners {
		fn()
	}
	return nil
}

func (r *Registry) Uninstall(name string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	next := make([]Extension, 0, len(r.extensions))
	for _, ext := range r.extensions {
		if ext.Name != name {
			next = append(next, ext)
		}
	}
	if len(next) == len(r.extensions) {
		r.mu.Unlock()
		return
	}
	r.extensions = next
	listeners := make([]func(), 0, len(r.listeners))
	for _, fn := range r.listeners {
		listeners = append(listeners, fn)
	}
	r.mu.Unlock()
	for _, fn := range listeners {
		fn()
	}
}

func (r *Registry) Subscribe(fn func()) func() {
	if r == nil || fn == nil {
		return func() {}
	}
	r.mu.Lock()
	id := r.next
	r.next++
	r.listeners[id] = fn
	r.mu.Unlock()
	return func() { r.mu.Lock(); delete(r.listeners, id); r.mu.Unlock() }
}

func (r *Registry) Snapshot() *RegistrySnapshot {
	if r == nil {
		s, _ := snapshotOf(nil)
		return s
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, _ := snapshotOf(r.extensions)
	return s
}

func snapshotOf(extensions []Extension) (*RegistrySnapshot, error) {
	exts := make([]Extension, len(extensions))
	for i, ext := range extensions {
		exts[i] = cloneExtension(ext)
	}
	tasks := make(map[string]TaskDefinition)
	for _, ext := range exts {
		for _, task := range ext.Tasks {
			if task.Kind == "" || tasks[task.Kind].Kind != "" {
				return nil, fmt.Errorf("%w: duplicate or empty task kind %q", ErrInvalid, task.Kind)
			}
			tasks[task.Kind] = task
		}
	}
	return &RegistrySnapshot{extensions: exts, tasks: tasks}, nil
}

func validateExtension(ext Extension) error {
	tools, sections := map[string]bool{}, map[string]bool{}
	for _, tool := range ext.Tools {
		if tool.Name == "" || tools[tool.Name] || tool.Execute == nil {
			return fmt.Errorf("%w: duplicate or empty tool name %q", ErrInvalid, tool.Name)
		}
		tools[tool.Name] = true
	}
	for _, task := range ext.Tasks {
		if task.Kind == "" || task.Version < 1 || task.Run == nil {
			return fmt.Errorf("%w: invalid task definition", ErrInvalid)
		}
	}
	for _, section := range ext.Sections {
		if !sectionNamePattern.MatchString(section.Name) || section.Name == "instructions" || sections[section.Name] {
			return fmt.Errorf("%w: invalid, reserved, or duplicate section %q", ErrInvalid, section.Name)
		}
		sections[section.Name] = true
	}
	return nil
}

func (s *RegistrySnapshot) Extensions() []Extension {
	if s == nil {
		return nil
	}
	result := make([]Extension, len(s.extensions))
	for i, ext := range s.extensions {
		result[i] = cloneExtension(ext)
	}
	return result
}
func (s *RegistrySnapshot) Task(kind string) (TaskDefinition, bool) {
	if s == nil {
		return TaskDefinition{}, false
	}
	t, ok := s.tasks[kind]
	return t, ok
}
func (s *RegistrySnapshot) Tools(agent Agent) []ToolDefinition {
	if s == nil {
		return nil
	}
	selected := selectedExtensions(s.extensions, agent.Extensions)
	tools := []ToolDefinition{}
	positions := map[string]int{}
	for _, ext := range selected {
		for _, tool := range ext.Tools {
			if i, ok := positions[tool.Name]; ok {
				tools[i] = tool
			} else {
				positions[tool.Name] = len(tools)
				tools = append(tools, tool)
			}
		}
	}
	for _, ext := range selected {
		for name, wrap := range ext.WrapTools {
			if i, ok := positions[name]; ok {
				if wrapped := wrap(tools[i]); wrapped.Name == name {
					tools[i] = wrapped
				} else {
					tools = append(tools[:i], tools[i+1:]...)
					delete(positions, name)
					for k, p := range positions {
						if p > i {
							positions[k] = p - 1
						}
					}
				}
			}
		}
	}
	if agent.Tools != nil {
		filtered := []ToolDefinition{}
		seen := map[string]bool{}
		for _, name := range agent.Tools {
			if i, ok := positions[name]; ok && !seen[name] {
				filtered = append(filtered, tools[i])
				seen[name] = true
			}
		}
		return filtered
	}
	return tools
}
func (s *RegistrySnapshot) Sections(agent Agent) []Section {
	if s == nil {
		return nil
	}
	out := []Section{}
	positions := map[string]int{}
	for _, ext := range selectedExtensions(s.extensions, agent.Extensions) {
		for _, sec := range ext.Sections {
			if i, ok := positions[sec.Name]; ok {
				out[i] = sec
			} else {
				positions[sec.Name] = len(out)
				out = append(out, sec)
			}
		}
	}
	return out
}
func (s *RegistrySnapshot) Hooks(agent Agent, kind string) []Hooks {
	var out []Hooks
	if s == nil {
		return out
	}
	for _, ext := range selectedExtensions(s.extensions, agent.Extensions) {
		if ext.Hooks.BeforeRequest != nil || ext.Hooks.AfterResponse != nil || ext.Hooks.BeforeTool != nil || ext.Hooks.AfterTool != nil || ext.Hooks.AfterTools != nil || ext.Hooks.OnYield != nil {
			out = append(out, ext.Hooks)
		}
	}
	return out
}
func cloneExtension(ext Extension) Extension {
	ext.Tools = append([]ToolDefinition(nil), ext.Tools...)
	for i := range ext.Tools {
		ext.Tools[i].Parameters = append([]byte(nil), ext.Tools[i].Parameters...)
	}
	ext.Sections = append([]Section(nil), ext.Sections...)
	ext.Tasks = append([]TaskDefinition(nil), ext.Tasks...)
	if ext.WrapTools != nil {
		wraps := make(map[string]func(ToolDefinition) ToolDefinition, len(ext.WrapTools))
		for key, fn := range ext.WrapTools {
			wraps[key] = fn
		}
		ext.WrapTools = wraps
	}
	return ext
}

func selectedExtensions(available []Extension, names []string) []Extension {
	if names == nil {
		return append([]Extension(nil), available...)
	}
	byName := map[string]Extension{}
	for _, e := range available {
		byName[e.Name] = e
	}
	out := []Extension{}
	seen := map[string]bool{}
	for _, name := range names {
		if e, ok := byName[name]; ok && !seen[name] {
			out = append(out, e)
			seen[name] = true
		}
	}
	return out
}
