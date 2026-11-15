package durable

import (
	"bytes"
	"os"
	"regexp"
	"testing"
)

// The README's quick start is examples/quickstart, which CI builds.
func TestReadmeQuickStartIsTheExample(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	example, err := os.ReadFile("examples/quickstart/main.go")
	if err != nil {
		t.Fatal(err)
	}
	match := regexp.MustCompile("(?s)```go\n(package main.*?)```").FindSubmatch(readme)
	if match == nil {
		t.Fatal("the README has no Go program")
	}
	if body := example[bytes.Index(example, []byte("package main")):]; !bytes.Equal(match[1], body) {
		t.Fatal("the README's quick start differs from examples/quickstart/main.go")
	}
}
