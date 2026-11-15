// Command quickstart is the README's quick start: a durable coding agent on
// SQLite, with a model from pi-ai's catalog. It needs GROQ_API_KEY.
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	durable "github.com/leocavalcante/pi-durable-go"
	"github.com/leocavalcante/pi-durable-go/model"
	"github.com/leocavalcante/pi-durable-go/storage"
	"github.com/leocavalcante/pi-durable-go/tools"
)

func main() {
	ctx := context.Background()

	// Any of pi-ai's 728 OpenAI-compatible models, configured as pi-ai does.
	llm, _ := model.OpenAIModel("groq", "llama-3.3-70b-versatile")
	llm.APIKey = os.Getenv("GROQ_API_KEY")

	// The coding tools: read, write, edit, and bash. A directory is not a sandbox!
	registry := durable.NewRegistry()
	if err := registry.Install(tools.Coding()); err != nil {
		log.Fatal(err)
	}

	// Every step is committed to SQLite before anyone sees it.
	store, err := storage.OpenSQLite(ctx, "agent.sqlite")
	if err != nil {
		log.Fatal(err)
	}
	h, err := durable.Open(ctx, store, durable.HarnessOptions{
		Model:    llm,
		Registry: registry,
		Environment: func(durable.ConversationRecord) (durable.ExecutionEnv, error) {
			return tools.NewLocalEnv(".")
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer h.Close(ctx)
	h.Resume() // finish whatever a previous run left unfinished

	agent, err := h.Root(ctx, durable.ConversationOptions{Agent: durable.Agent{
		Model: durable.ModelRef{Provider: "groq", ID: "llama-3.3-70b-versatile"},
	}})
	if err != nil {
		log.Fatal(err)
	}

	// The request ID admits the task once, even across restarts.
	task, err := agent.Submit(ctx, durable.SubmissionRequest{
		Content:   "Which Go packages does this directory have?",
		RequestID: "inventory-1",
	})
	if err != nil {
		log.Fatal(err)
	}
	result, err := task.Wait(ctx)
	if err != nil {
		log.Fatal(err)
	}

	entries, err := agent.Entries(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, entry := range entries {
		if entry.ID == result.Answer {
			fmt.Println(entry.Message.Content)
		}
	}
}
