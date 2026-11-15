// Command recovery shows a conversation surviving a crash. A scripted model
// asks for three steps of a job, one tool call at a time. With -crash, the
// process dies in the middle of step 2; run it again, and it opens the same
// SQLite file and finishes the job where it stopped. No network is used.
//
//	go run ./examples/recovery -crash   # dies in the middle of step 2
//	go run ./examples/recovery          # picks up the job and finishes it
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	durable "github.com/leocavalcante/pi-durable-go"
	"github.com/leocavalcante/pi-durable-go/storage"
)

// planner is a scripted model: it calls the step tool for steps 1, 2, and 3,
// one at a time, then answers.
type planner struct{}

func (planner) Generate(ctx context.Context, request durable.ModelRequest, _ func(string) error) (durable.ModelResponse, error) {
	if err := ctx.Err(); err != nil {
		return durable.ModelResponse{}, err
	}
	done := 0
	for _, message := range request.Messages {
		if message.Role == "tool" {
			done++
		}
	}
	if done < 3 {
		step := done + 1
		return durable.ModelResponse{ToolCalls: []durable.ToolCall{{
			ID: fmt.Sprintf("call-%d", step), Name: "step", Arguments: json.RawMessage(fmt.Sprintf(`{"n":%d}`, step)),
		}}}, nil
	}
	return durable.ModelResponse{Content: "All three steps are done."}, nil
}

func main() {
	crash := flag.Bool("crash", false, "exit in the middle of step 2, as a crash would")
	db := flag.String("db", filepath.Join(os.TempDir(), "pi-durable-recovery.sqlite"), "SQLite file")
	fresh := flag.Bool("fresh", false, "start over with a new database")
	flag.Parse()
	if *fresh || *crash {
		for _, suffix := range []string{"", "-wal", "-shm"} {
			_ = os.Remove(*db + suffix)
		}
	}
	ctx := context.Background()

	// The step tool. ReplaySafe says it may run again after a crash
	// interrupted it; an unsafe tool's interrupted call ends with an error
	// result instead, because its effects are unknown.
	registry := durable.NewRegistry()
	if err := registry.Install(durable.Extension{Name: "job", Tools: []durable.ToolDefinition{{
		ToolSpec: durable.ToolSpec{
			Name:        "step",
			Description: "Runs one step of the job.",
			Parameters:  json.RawMessage(`{"type":"object","properties":{"n":{"type":"integer"}},"required":["n"]}`),
		},
		ReplaySafe: true,
		Execute: func(ctx context.Context, args json.RawMessage, rt *durable.ToolRuntime) (durable.ToolResult, error) {
			var step struct{ N int }
			if err := json.Unmarshal(args, &step); err != nil {
				return durable.ToolResult{}, err
			}
			fmt.Printf("  step %d: running\n", step.N)
			if *crash && step.N == 2 {
				fmt.Println("  step 2: the process dies here")
				os.Exit(1)
			}
			select {
			case <-time.After(300 * time.Millisecond):
			case <-ctx.Done():
				return durable.ToolResult{}, ctx.Err()
			}
			fmt.Printf("  step %d: done\n", step.N)
			return durable.ToolResult{Content: fmt.Sprintf("step %d done", step.N)}, nil
		},
	}}}); err != nil {
		log.Fatal(err)
	}

	store, err := storage.OpenSQLite(ctx, *db)
	if err != nil {
		log.Fatal(err)
	}
	h, err := durable.Open(ctx, store, durable.HarnessOptions{Model: planner{}, Registry: registry})
	if err != nil {
		store.Close()
		log.Fatal(err)
	}
	defer h.Close(ctx)
	// Scheduling starts here: tasks a crash interrupted run again.
	h.Resume()

	root, err := h.Root(ctx, durable.ConversationOptions{Agent: durable.Agent{Model: durable.ModelRef{Provider: "example", ID: "planner"}}})
	if err != nil {
		log.Fatal(err)
	}
	// The request ID admits the job once: after a crash, submitting again
	// returns the same submission, which the recovered run settles.
	submission, err := root.Submit(ctx, durable.SubmissionRequest{Content: "Run the job's three steps.", RequestID: "job-1"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("job %d: waiting for it to finish (%s)\n", submission.ID(), *db)
	result, err := submission.Wait(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("job %d: %s\n\n", submission.ID(), result.Status)

	entries, err := root.Entries(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("transcript:")
	for _, entry := range entries {
		if entry.Message == nil {
			continue
		}
		text := entry.Message.Content
		for _, call := range entry.Message.ToolCalls {
			text += fmt.Sprintf("%s(%s)", call.Name, call.Arguments)
		}
		// The prompt and the tools each request offered.
		for _, tool := range entry.Message.ToolsAdded {
			text += "tools: " + tool.Name
		}
		fmt.Printf("  %-9s %s\n", entry.Message.Role, text)
	}
}
