package codemode

// The QuickJS VM of QuickJSSandbox: QuickJS-NG compiled to WebAssembly
// (quickjs-wasi 3.6.2, as installed with Pi 1.5.2) through the wazero
// runtime, wrapped as quickjs-wasi wraps it.

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

//go:embed quickjs.wasm
var quickjsWasm []byte

// preludeSource is pi-codemode 1.5.2's PRELUDE_SOURCE: the script
// environment (tools, globals, output, store), evaluated before the script.
//
//go:embed prelude.js
var preludeSource string

const (
	// quickjsWasiVersion is the quickjs-wasi release the module comes from.
	quickjsWasiVersion = "3.6.2"
	// maxStackSize is quickjs-wasi's MAX_STACK_SIZE, which vm.js sets.
	maxStackSize = 512 * 1024
	// intrinsics are all but WeakRef and FinalizationRegistry, for
	// deterministic runs (Intrinsics.ALL & ~Intrinsics.WEAK_REF).
	intrinsics = 0xFFFFFFFF &^ (1 << 9)
)

// errInterrupted ends a drain that the host interrupted.
var errInterrupted = errors.New("Execution interrupted")

// engine compiles the module once; each run instantiates it.
type engine struct {
	runtime  wazero.Runtime
	compiled wazero.CompiledModule
	// vms finds a run's VM from the module calling a host function.
	vms sync.Map // api.Module -> *vm
}

func newEngine(ctx context.Context) (*engine, error) {
	runtime := wazero.NewRuntime(ctx)
	e := &engine{runtime: runtime}
	byModule := func(m api.Module) *vm {
		value, _ := e.vms.Load(m)
		v, _ := value.(*vm)
		return v
	}
	i32 := api.ValueTypeI32
	_, err := runtime.NewHostModuleBuilder("env").
		NewFunctionBuilder().WithGoModuleFunction(api.GoModuleFunc(func(ctx context.Context, m api.Module, stack []uint64) {
		stack[0] = uint64(byModule(m).hostCall(uint32(stack[0]), uint32(stack[1]), uint32(stack[2]), uint32(stack[3]), uint32(stack[4])))
	}), []api.ValueType{i32, i32, i32, i32, i32}, []api.ValueType{i32}).Export("host_call").
		NewFunctionBuilder().WithGoModuleFunction(api.GoModuleFunc(func(ctx context.Context, m api.Module, stack []uint64) {
		stack[0] = 0
		if v := byModule(m); v != nil && v.interrupt() {
			stack[0] = 1
		}
	}), nil, []api.ValueType{i32}).Export("host_interrupt").
		NewFunctionBuilder().WithGoModuleFunction(api.GoModuleFunc(func(ctx context.Context, m api.Module, stack []uint64) {
		// No rejection handler is installed: free both values, as
		// quickjs-wasi does without one.
		if v := byModule(m); v != nil {
			v.call("qjs_free_value", stack[0])
			v.call("qjs_free_value", stack[1])
		}
	}), []api.ValueType{i32, i32, i32}, nil).Export("host_promise_rejection").
		NewFunctionBuilder().WithGoModuleFunction(api.GoModuleFunc(func(ctx context.Context, m api.Module, stack []uint64) {
		stack[0] = 0 // no module loader
	}), []api.ValueType{i32, i32}, []api.ValueType{i32}).Export("host_module_normalize").
		NewFunctionBuilder().WithGoModuleFunction(api.GoModuleFunc(func(ctx context.Context, m api.Module, stack []uint64) {
		stack[0] = 0
	}), []api.ValueType{i32, i32}, []api.ValueType{i32}).Export("host_module_load").
		NewFunctionBuilder().WithGoModuleFunction(api.GoModuleFunc(func(ctx context.Context, m api.Module, stack []uint64) {
		stack[0] = 0 // UTC: timezoneOffset 0
	}), []api.ValueType{i32, i32}, []api.ValueType{i32}).Export("host_get_timezone_offset").
		Instantiate(ctx)
	if err != nil {
		return nil, err
	}
	if err := e.wasi(ctx); err != nil {
		return nil, err
	}
	e.compiled, err = runtime.CompileModule(ctx, quickjsWasm)
	if err != nil {
		return nil, err
	}
	return e, nil
}

// wasi is quickjs-wasi's WASI shim with vm.js's deterministic clock and
// random source and discarded output (deterministicWasi, discardOutput).
func (e *engine) wasi(ctx context.Context) error {
	byModule := func(m api.Module) *vm {
		value, _ := e.vms.Load(m)
		v, _ := value.(*vm)
		return v
	}
	i32, i64 := api.ValueTypeI32, api.ValueTypeI64
	const errnoSuccess, errnoBadf, errnoNosys = 0, 8, 52
	_, err := e.runtime.NewHostModuleBuilder("wasi_snapshot_preview1").
		NewFunctionBuilder().WithGoModuleFunction(api.GoModuleFunc(func(ctx context.Context, m api.Module, stack []uint64) {
		clockID, result := uint32(stack[0]), uint32(stack[2])
		var nanoseconds uint64
		if v := byModule(m); v != nil {
			c := &v.clock
			switch {
			case c.creating && clockID == 0:
				// QuickJS seeds Math.random() from this reading in
				// microseconds and turns 0 into 1, so seed s reads s + 1.
				nanoseconds = (uint64(c.seed) + 1) * 1000
			case c.creating:
				nanoseconds = 0
			case clockID == 0:
				nanoseconds = uint64(int64(jsRound(c.now))) * 1000000
			default:
				nanoseconds = uint64(int64(jsRound(c.now-c.startedAt))) * 1000000
			}
		}
		m.Memory().WriteUint64Le(result, nanoseconds)
		stack[0] = errnoSuccess
	}), []api.ValueType{i32, i64, i32}, []api.ValueType{i32}).Export("clock_time_get").
		NewFunctionBuilder().WithGoModuleFunction(api.GoModuleFunc(func(ctx context.Context, m api.Module, stack []uint64) {
		stack[0] = errnoNosys
	}), []api.ValueType{i32}, []api.ValueType{i32}).Export("fd_close").
		NewFunctionBuilder().WithGoModuleFunction(api.GoModuleFunc(func(ctx context.Context, m api.Module, stack []uint64) {
		fd, stat := uint32(stack[0]), uint32(stack[1])
		if fd != 1 && fd != 2 {
			stack[0] = errnoBadf
			return
		}
		memory := m.Memory()
		memory.WriteByte(stat, 2) // character device
		memory.WriteUint16Le(stat+2, 0)
		memory.WriteUint64Le(stat+8, 0)
		memory.WriteUint64Le(stat+16, 0)
		stack[0] = errnoSuccess
	}), []api.ValueType{i32, i32}, []api.ValueType{i32}).Export("fd_fdstat_get").
		NewFunctionBuilder().WithGoModuleFunction(api.GoModuleFunc(func(ctx context.Context, m api.Module, stack []uint64) {
		stack[0] = errnoNosys
	}), []api.ValueType{i32, i64, i32, i32}, []api.ValueType{i32}).Export("fd_seek").
		NewFunctionBuilder().WithGoModuleFunction(api.GoModuleFunc(func(ctx context.Context, m api.Module, stack []uint64) {
		// Engine diagnostics belong to nobody: every byte counts as written.
		iovs, count, written := uint32(stack[1]), uint32(stack[2]), uint32(stack[3])
		memory := m.Memory()
		total := uint32(0)
		for i := uint32(0); i < count; i++ {
			length, _ := memory.ReadUint32Le(iovs + i*8 + 4)
			total += length
		}
		memory.WriteUint32Le(written, total)
		stack[0] = errnoSuccess
	}), []api.ValueType{i32, i32, i32, i32}, []api.ValueType{i32}).Export("fd_write").
		NewFunctionBuilder().WithGoModuleFunction(api.GoModuleFunc(func(ctx context.Context, m api.Module, stack []uint64) {
		buffer, length := uint32(stack[0]), uint32(stack[1])
		bytes, ok := m.Memory().Read(buffer, length)
		if v := byModule(m); ok && v != nil {
			for i := uint32(0); i < length; i += 4 {
				value := v.clock.random()
				for b := uint32(0); b < 4 && i+b < length; b++ {
					bytes[i+b] = byte(value >> (b * 8))
				}
			}
		}
		stack[0] = errnoSuccess
	}), []api.ValueType{i32, i32}, []api.ValueType{i32}).Export("random_get").
		Instantiate(ctx)
	return err
}

func (e *engine) close(ctx context.Context) { _ = e.runtime.Close(ctx) }

// jsRound is Math.round: half up.
func jsRound(x float64) float64 { return math.Floor(x + 0.5) }

// clock is vm.js's deterministic clock and random source.
type clock struct {
	creating        bool
	startedAt, now  float64
	seed, prng      uint32
	prngInitialized bool
}

// random is mulberry32 over the seed (vm.js prng, seeded with seed ^
// 0x9e3779b9): one 32-bit draw per call.
func (c *clock) random() uint32 {
	if !c.prngInitialized {
		c.prng, c.prngInitialized = c.seed^0x9e3779b9, true
	}
	c.prng += 0x6d2b79f5
	value := (c.prng ^ c.prng>>15) * (1 | c.prng)
	value = (value + (value^value>>7)*(61|value)) ^ value
	return value ^ value>>14
}

// vm is one QuickJS instance; it is used from one goroutine at a time.
type vm struct {
	ctx       context.Context
	engine    *engine
	module    api.Module
	clock     clock
	interrupt func() bool
	callbacks map[string]func(args []uint32) error
	exports   map[string]api.Function
	// trap is a host-side failure inside a call into the VM.
	trap error
}

func (e *engine) newVM(ctx context.Context, memoryLimit int64, startedAt float64, seed uint32, interrupt func() bool) (*vm, error) {
	module, err := e.runtime.InstantiateModule(ctx, e.compiled, wazero.NewModuleConfig().WithName("").WithStartFunctions())
	if err != nil {
		return nil, err
	}
	v := &vm{ctx: ctx, engine: e, module: module, interrupt: interrupt, callbacks: map[string]func([]uint32) error{},
		exports: map[string]api.Function{}, clock: clock{creating: true, startedAt: startedAt, now: startedAt, seed: seed}}
	e.vms.Store(module, v)
	if _, err := v.callErr("_initialize"); err != nil {
		v.close()
		return nil, err
	}
	result, err := v.callErr("qjs_init2", uint64(uint32(intrinsics)))
	if err != nil || result != 0 {
		v.close()
		return nil, errors.New("Failed to initialize QuickJS runtime")
	}
	v.clock.creating = false
	if memoryLimit > 0 {
		v.call("qjs_set_memory_limit", uint64(uint32(memoryLimit)))
	}
	v.call("qjs_set_max_stack_size", maxStackSize)
	v.call("qjs_set_interrupt_handler", 1)
	return v, v.trap
}

func (v *vm) close() {
	v.engine.vms.Delete(v.module)
	_ = v.module.Close(v.ctx)
}

func (v *vm) export(name string) api.Function {
	fn, ok := v.exports[name]
	if !ok {
		fn = v.module.ExportedFunction(name)
		v.exports[name] = fn
	}
	return fn
}

func (v *vm) callErr(name string, args ...uint64) (uint64, error) {
	results, err := v.export(name).Call(v.ctx, args...)
	if err != nil {
		return 0, err
	}
	if len(results) == 0 {
		return 0, nil
	}
	return results[0], nil
}

// call calls an export; a failure (a trap, or the run's context ending) is
// kept as the VM's trap and reads as 0.
func (v *vm) call(name string, args ...uint64) uint32 {
	if v.trap != nil {
		return 0
	}
	result, err := v.callErr(name, args...)
	if err != nil {
		v.trap = err
		return 0
	}
	return uint32(result)
}

// writeString writes s as a NUL-terminated string the caller frees.
func (v *vm) writeString(s string) (uint32, uint32) {
	ptr := v.call("wasm_malloc", uint64(len(s)+1))
	if ptr == 0 {
		if v.trap == nil {
			v.trap = errors.New("wasm_malloc failed")
		}
		return 0, 0
	}
	memory := v.module.Memory()
	memory.WriteString(ptr, s)
	memory.WriteByte(ptr+uint32(len(s)), 0)
	return ptr, uint32(len(s))
}

func (v *vm) cString(ptr uint32) string {
	memory := v.module.Memory()
	var out []byte
	for {
		b, ok := memory.ReadByte(ptr)
		if !ok || b == 0 {
			return string(out)
		}
		out = append(out, b)
		ptr++
	}
}

// Values are pointers to the module's heap boxes (quickjs-wasi's handles).

func (v *vm) undefined() uint32 { return v.call("qjs_get_undefined") }
func (v *vm) boolean(b bool) uint32 {
	if b {
		return v.call("qjs_get_true")
	}
	return v.call("qjs_get_false")
}
func (v *vm) free(value uint32) {
	if value != 0 {
		v.call("qjs_free_value", uint64(value))
	}
}
func (v *vm) newString(s string) uint32 {
	ptr, length := v.writeString(s)
	if ptr == 0 {
		return 0
	}
	value := v.call("qjs_new_string", uint64(ptr), uint64(length))
	v.call("wasm_free", uint64(ptr))
	return value
}
func (v *vm) newNumber(f float64) uint32 {
	return v.call("qjs_new_number", api.EncodeF64(f))
}
func (v *vm) isUndefined(value uint32) bool { return v.call("qjs_is_undefined", uint64(value)) != 0 }
func (v *vm) toNumber(value uint32) float64 {
	if v.trap != nil {
		return 0
	}
	result, err := v.callErr("qjs_get_float64", uint64(value))
	if err != nil {
		v.trap = err
		return 0
	}
	return api.DecodeF64(result)
}
func (v *vm) toBoolean(value uint32) bool { return v.call("qjs_get_bool", uint64(value)) != 0 }

// toString is the value as a string, its WTF-8 decoded as JavaScript text.
func (v *vm) toString(value uint32) string {
	lengthPtr := v.call("wasm_malloc", 4)
	if lengthPtr == 0 {
		return ""
	}
	defer v.call("wasm_free", uint64(lengthPtr))
	cstr := v.call("qjs_get_string_len", uint64(value), uint64(lengthPtr))
	if cstr == 0 {
		return "<null>"
	}
	memory := v.module.Memory()
	length, _ := memory.ReadUint32Le(lengthPtr)
	bytes, _ := memory.Read(cstr, length)
	text := wtf8String(bytes)
	v.call("qjs_free_cstring", uint64(cstr))
	return text
}

// wtf8String is JavaScript text from the VM's WTF-8 as Go reads it from
// pi-codemode's JSON messages: a lone surrogate, which UTF-8 cannot hold,
// is one U+FFFD.
func wtf8String(bytes []byte) string {
	if utf8.Valid(bytes) {
		return string(bytes)
	}
	var out strings.Builder
	for i := 0; i < len(bytes); {
		if bytes[i] == 0xED && i+2 < len(bytes) && bytes[i+1] >= 0xA0 && bytes[i+1] <= 0xBF {
			out.WriteRune(utf8.RuneError)
			i += 3
			continue
		}
		r, size := utf8.DecodeRune(bytes[i:])
		out.WriteRune(r)
		i += size
	}
	return out.String()
}

func (v *vm) getProp(object uint32, name string) uint32 {
	ptr, _ := v.writeString(name)
	if ptr == 0 {
		return 0
	}
	value := v.call("qjs_get_prop_string", uint64(object), uint64(ptr))
	v.call("wasm_free", uint64(ptr))
	return value
}

// jsError is an exception the VM threw: its name, message, and stack.
type jsError struct {
	name, message, stack string
	hasStack             bool
}

func (e *jsError) Error() string { return e.name + ": " + e.message }

// exception reads the pending exception, as quickjs-wasi's JSException
// does.
func (v *vm) exception() *jsError {
	handle := v.call("qjs_get_exception")
	defer v.free(handle)
	out := &jsError{name: "Error"}
	name := v.getProp(handle, "name")
	if !v.isUndefined(name) {
		out.name = v.toString(name)
	}
	v.free(name)
	message := v.getProp(handle, "message")
	if v.isUndefined(message) {
		out.message = v.toString(handle)
	} else {
		out.message = v.toString(message)
	}
	v.free(message)
	stack := v.getProp(handle, "stack")
	if !v.isUndefined(stack) {
		out.stack, out.hasStack = v.toString(stack), true
	}
	v.free(stack)
	return out
}

// checked is a result, or the exception it signals.
func (v *vm) checked(result uint32) (uint32, error) {
	if v.trap != nil {
		return 0, v.trap
	}
	if v.call("qjs_is_exception", uint64(result)) != 0 {
		v.free(result)
		return 0, v.exception()
	}
	return result, v.trap
}

func (v *vm) eval(code, filename string) (uint32, error) {
	codePtr, codeLength := v.writeString(code)
	filenamePtr, _ := v.writeString(filename)
	result := v.call("qjs_eval", uint64(codePtr), uint64(codeLength), uint64(filenamePtr), 0)
	v.call("wasm_free", uint64(codePtr))
	v.call("wasm_free", uint64(filenamePtr))
	return v.checked(result)
}

func (v *vm) callFunction(fn, this uint32, args ...uint32) (uint32, error) {
	argv := uint32(0)
	if len(args) > 0 {
		argv = v.call("wasm_malloc", uint64(4*len(args)))
		memory := v.module.Memory()
		for i, arg := range args {
			memory.WriteUint32Le(argv+uint32(4*i), arg)
		}
	}
	result := v.call("qjs_call", uint64(fn), uint64(this), uint64(len(args)), uint64(argv))
	if argv != 0 {
		v.call("wasm_free", uint64(argv))
	}
	return v.checked(result)
}

// newFunction is a function that calls back into Go with its arguments.
func (v *vm) newFunction(name string, callback func(args []uint32) error) uint32 {
	v.callbacks[name] = callback
	ptr, length := v.writeString(name)
	fn := v.call("qjs_new_host_function", uint64(ptr), uint64(length), 0)
	v.call("wasm_free", uint64(ptr))
	return fn
}

// hostCall dispatches a call of a host function. Its arguments belong to the
// caller; it returns a value the caller takes, or 0 after throwing.
func (v *vm) hostCall(namePtr, nameLength, this, argc, argv uint32) uint32 {
	bytes, _ := v.module.Memory().Read(namePtr, nameLength)
	callback := v.callbacks[string(bytes)]
	if callback == nil {
		return v.throw(fmt.Sprintf("Host callback %q is not registered", string(bytes)))
	}
	args := make([]uint32, argc)
	memory := v.module.Memory()
	for i := range args {
		args[i], _ = memory.ReadUint32Le(argv + uint32(4*i))
	}
	if err := callback(args); err != nil {
		return v.throw(err.Error())
	}
	return v.call("qjs_dup_value", uint64(v.undefined()))
}

func (v *vm) throw(message string) uint32 {
	err := v.call("qjs_new_error")
	text := v.newString(message)
	ptr, _ := v.writeString("message")
	v.call("qjs_set_prop_string", uint64(err), uint64(ptr), uint64(text))
	v.call("wasm_free", uint64(ptr))
	v.free(text)
	v.call("qjs_throw", uint64(err))
	v.free(err)
	return 0
}

// quickjsVersion is the engine's version string.
func (v *vm) quickjsVersion() string { return v.cString(v.call("qjs_get_quickjs_version")) }

// fnv1a hashes UTF-16 code units as 8 hex digits (vm.js identifies the
// prelude with it).
func fnv1a(text string) string {
	hash := uint32(0x811c9dc5)
	for _, unit := range utf16.Encode([]rune(text)) {
		hash ^= uint32(unit)
		hash *= 0x01000193
	}
	return fmt.Sprintf("%08x", hash)
}

var preludeHash = fnv1a(preludeSource)
