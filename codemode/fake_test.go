package codemode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// fakeSandbox runs Go scripts with the VM protocol's rules, so make check
// tests the durable side without Node: it reports its fingerprint first,
// idle with the calls issued after the first run and after each delivered
// result, applies results in seq order, and ends with where it was.
type fakeSandbox struct {
	fingerprint string
	scripts     map[string]fakeScript
	mu          sync.Mutex
	starts      int
}

// fakeScript is a script: it makes calls with vm.call and waits for their
// results with vm.wait; its return value is the script's (nil is
// undefined), its error the script's error.
type fakeScript func(vm *fakeVM) (json.RawMessage, error)

func (s *fakeSandbox) Start(code string, options RunOptions) (Run, error) {
	script, ok := s.scripts[code]
	if !ok {
		return nil, fmt.Errorf("no fake script %q", code)
	}
	s.mu.Lock()
	s.starts++
	s.mu.Unlock()
	vm := &fakeVM{options: options, results: map[int]Outcome{}, deliveries: make(chan delivery, 1024),
		aborts: make(chan string, 1), done: make(chan struct{}), pending: map[int]bool{}, delivered: map[int]bool{},
		clock: options.StartedAt}
	go vm.run(script, s.fingerprint)
	return vm, nil
}

type delivery struct {
	seq     int
	now     float64
	id      int
	outcome Outcome
}

// errAborted ends a script that was aborted.
var errAborted = errors.New("aborted")

type fakeVM struct {
	options RunOptions
	// Owned by the script's goroutine.
	issued  int
	current int
	nextSeq int
	clock   float64
	early   map[int]delivery
	results map[int]Outcome
	output  []OutputItem
	store   map[string]json.RawMessage
	deleted []string

	deliveries chan delivery
	aborts     chan string

	mu        sync.Mutex
	pending   map[int]bool
	delivered map[int]bool
	finished  bool
	result    Result
	done      chan struct{}
}

func (vm *fakeVM) run(script fakeScript, fingerprint string) {
	vm.nextSeq, vm.early = 1, map[int]delivery{}
	vm.store = map[string]json.RawMessage{}
	vm.options.OnStarted(fingerprint)
	var value json.RawMessage
	var err error
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				if recovered == errAborted {
					err = errAborted
					return
				}
				panic(recovered)
			}
		}()
		value, err = script(vm)
	}()
	during := &During{Seq: vm.current, Issued: vm.issued}
	switch {
	case err == errAborted:
		return // settled by abort
	case err != nil:
		vm.settle(Result{OK: false, Error: &Error{Kind: "script", Name: "Error", Message: err.Error(), Stack: "Error: " + err.Error()}, During: during})
	default:
		set := map[string]json.RawMessage{}
		for key, value := range vm.store {
			set[key] = value
		}
		vm.settle(Result{OK: true, Value: value, StoreWrites: StoreWrites{Set: set, Delete: append([]string{}, vm.deleted...)}, During: during})
	}
}

func (vm *fakeVM) settle(result Result) {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	if vm.finished {
		return
	}
	if result.Output == nil {
		result.Output = append([]OutputItem{}, vm.output...)
	}
	if result.Calls == nil {
		result.Calls = []CallRecord{}
	}
	vm.finished, vm.result = true, result
	close(vm.done)
}

// call makes a call: target "tool" or "global", with JSON arguments ("" for
// undefined); it returns the call's ID.
func (vm *fakeVM) call(target, name, args string) int {
	vm.issued++
	id := vm.issued
	vm.mu.Lock()
	vm.pending[id] = true
	vm.mu.Unlock()
	var raw json.RawMessage
	if args != "" {
		raw = json.RawMessage(args)
	}
	vm.options.OnCall(HostCall{ID: id, Target: target, Name: name, Args: raw})
	return id
}

// wait waits for call id's outcome: the VM is idle until results arrive,
// which it applies in seq order, reporting idle after each one that does not
// answer id.
func (vm *fakeVM) wait(id int) Outcome {
	if outcome, ok := vm.results[id]; ok {
		return outcome
	}
	vm.options.OnIdle(vm.current, vm.issued)
	for {
		select {
		case message := <-vm.aborts:
			vm.settle(Result{OK: false, Error: &Error{Kind: "aborted", Message: message}})
			panic(errAborted)
		case d := <-vm.deliveries:
			vm.early[d.seq] = d
		}
		for next, ok := vm.early[vm.nextSeq]; ok; next, ok = vm.early[vm.nextSeq] {
			delete(vm.early, vm.nextSeq)
			vm.clock = max(vm.clock, next.now)
			vm.current = vm.nextSeq
			vm.nextSeq++
			vm.results[next.id] = next.outcome
			if next.id == id {
				return next.outcome
			}
			vm.options.OnIdle(vm.current, vm.issued)
		}
	}
}

// now is the VM clock.
func (vm *fakeVM) now() float64 { return vm.clock }

func (vm *fakeVM) text(text string) {
	vm.output = append(vm.output, OutputItem{Type: "text", Text: text})
}

func (vm *fakeVM) load(key string) json.RawMessage { return vm.options.Store[key] }

func (vm *fakeVM) storeValue(key string, value json.RawMessage) { vm.store[key] = value }

func (vm *fakeVM) Deliver(seq int, now float64, id int, outcome Outcome) error {
	vm.mu.Lock()
	if vm.finished {
		vm.mu.Unlock()
		return nil
	}
	if err := checkDelivery(seq, now, vm.delivered, vm.pending, id); err != nil {
		vm.mu.Unlock()
		return err
	}
	vm.delivered[seq] = true
	delete(vm.pending, id)
	vm.mu.Unlock()
	vm.deliveries <- delivery{seq: seq, now: now, id: id, outcome: outcome}
	return nil
}

func (vm *fakeVM) Abort(message string) {
	select {
	case vm.aborts <- message:
	default:
	}
	// A script that is not waiting ends when it next waits; one that never
	// waits again settles here.
	go func() {
		<-vm.done
	}()
}

func (vm *fakeVM) Finished() bool {
	vm.mu.Lock()
	defer vm.mu.Unlock()
	return vm.finished
}

func (vm *fakeVM) Wait(ctx context.Context) (Result, error) {
	select {
	case <-vm.done:
		return vm.result, nil
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
}
